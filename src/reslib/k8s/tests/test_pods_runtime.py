from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kubernetes.client.exceptions import ApiException

from reslib.k8s import pods as pod_helpers
from reslib.k8s.exceptions import ContainerCrashedError


def _ready_condition(*, status="True", at=None):
    return SimpleNamespace(
        type="Ready",
        status=status,
        last_transition_time=at,
    )


def _pod(
    *,
    name="pod-a",
    phase="Running",
    conditions=None,
    grace=None,
    container_statuses=None,
):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(termination_grace_period_seconds=grace),
        status=SimpleNamespace(
            phase=phase,
            conditions=conditions or [],
            container_statuses=container_statuses or [],
        ),
    )


def _container_status(*, name="app", waiting=None, terminated=None):
    return SimpleNamespace(
        name=name,
        state=SimpleNamespace(waiting=waiting, terminated=terminated),
    )


def test_get_latest_pod_ready_time_returns_most_recent_timestamp():
    first = datetime.now(timezone.utc)
    second = first + timedelta(seconds=10)
    pods = [
        _pod(conditions=[_ready_condition(at=first)]),
        _pod(conditions=[_ready_condition(at=second)]),
        _pod(conditions=[_ready_condition(status="False", at=second)]),
    ]

    assert pod_helpers.get_latest_pod_ready_time(pods) == second
    assert pod_helpers.get_latest_pod_ready_time([_pod()]) is None


def test_is_pod_ready_detects_ready_condition():
    assert pod_helpers.is_pod_ready(_pod(conditions=[_ready_condition()])) is True
    assert (
        pod_helpers.is_pod_ready(_pod(conditions=[_ready_condition(status="False")]))
        is False
    )


@pytest.mark.asyncio
async def test_get_pods_by_labels_builds_selector_and_filters_phase():
    pods = SimpleNamespace(
        items=[_pod(name="a", phase="Running"), _pod(name="b", phase="Pending")]
    )
    fake_k8s = SimpleNamespace(list_namespaced_pod=AsyncMock(return_value=pods))

    running = await pod_helpers.get_pods_by_labels(
        k8s=fake_k8s,
        namespace="payments",
        labels={"app": "checkout"},
    )
    all_pods = await pod_helpers.get_pods_by_labels(
        k8s=fake_k8s,
        namespace="payments",
        labels={"app": "checkout"},
        pod_phase=None,
    )

    fake_k8s.list_namespaced_pod.assert_awaited()
    assert [pod.metadata.name for pod in running] == ["a"]
    assert [pod.metadata.name for pod in all_pods] == ["a", "b"]


@pytest.mark.asyncio
async def test_get_workload_pods_delegates_to_label_lookup(monkeypatch):
    get_pods = AsyncMock(return_value=[_pod(name="a")])
    monkeypatch.setattr(pod_helpers, "get_pods_by_labels", get_pods)
    workload_spec = SimpleNamespace(labels={"app": "checkout"})

    pods = await pod_helpers.get_workload_pods(
        k8s=SimpleNamespace(),
        namespace="payments",
        workload_spec=workload_spec,
        pod_phase="Pending",
    )

    assert [pod.metadata.name for pod in pods] == ["a"]
    get_pods.assert_awaited_once()


@pytest.mark.asyncio
async def test_pod_exists_handles_found_not_found_and_other_errors(monkeypatch):
    fake_k8s = SimpleNamespace(read_namespaced_pod=AsyncMock(return_value=_pod()))
    monkeypatch.setattr(pod_helpers, "KubernetesClient", lambda: fake_k8s)

    assert await pod_helpers.pod_exists(namespace="payments", pod_name="pod-a") is True

    fake_k8s.read_namespaced_pod = AsyncMock(
        side_effect=ApiException(status=404, reason="not found")
    )
    assert (
        await pod_helpers.pod_exists(
            namespace="payments", pod_name="pod-a", k8s=fake_k8s
        )
        is False
    )

    fake_k8s.read_namespaced_pod = AsyncMock(
        side_effect=ApiException(status=500, reason="boom")
    )
    with pytest.raises(ApiException):
        await pod_helpers.pod_exists(
            namespace="payments", pod_name="pod-a", k8s=fake_k8s
        )


def test_get_pod_termination_timeout_uses_max_grace_and_caps_timeout():
    pods = [_pod(grace=20), _pod(grace=None)]

    assert pod_helpers.get_pod_termination_timeout(pods, buffer_seconds=5) == 35
    assert (
        pod_helpers.get_pod_termination_timeout(
            [_pod(grace=500)],
            buffer_seconds=10,
            max_timeout=300,
        )
        == 300
    )

    with pytest.raises(ValueError, match="No pods given"):
        pod_helpers.get_pod_termination_timeout([])


@pytest.mark.asyncio
async def test_raise_on_container_fail_detects_waiting_and_terminated_states(
    monkeypatch,
):
    healthy_pod = _pod(
        container_statuses=[
            _container_status(
                waiting=SimpleNamespace(reason="ContainerCreating"),
                terminated=None,
            ),
            _container_status(
                waiting=None,
                terminated=SimpleNamespace(reason="Completed", exit_code=0),
            ),
        ]
    )
    get_pods = AsyncMock(return_value=[healthy_pod])
    monkeypatch.setattr(pod_helpers, "get_workload_pods", get_pods)

    assert (
        await pod_helpers.raise_on_container_fail(
            k8s=SimpleNamespace(),
            workload_spec=SimpleNamespace(),
            namespace="payments",
        )
        is None
    )

    waiting_pod = _pod(
        name="pod-wait",
        container_statuses=[
            _container_status(
                waiting=SimpleNamespace(reason="CrashLoopBackOff"),
                terminated=None,
            )
        ],
    )
    get_pods.return_value = [waiting_pod]
    with pytest.raises(ContainerCrashedError, match="waiting state 'CrashLoopBackOff'"):
        await pod_helpers.raise_on_container_fail(
            k8s=SimpleNamespace(),
            workload_spec=SimpleNamespace(),
            namespace="payments",
        )

    terminated_pod = _pod(
        name="pod-term",
        container_statuses=[
            _container_status(
                waiting=None,
                terminated=SimpleNamespace(reason="Error", exit_code=137),
            )
        ],
    )
    get_pods.return_value = [terminated_pod]
    with pytest.raises(ContainerCrashedError, match="terminated with reason 'Error'"):
        await pod_helpers.raise_on_container_fail(
            k8s=SimpleNamespace(),
            workload_spec=SimpleNamespace(),
            namespace="payments",
        )
