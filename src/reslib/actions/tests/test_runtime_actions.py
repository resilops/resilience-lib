import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from reslib.actions import (
    endpoint as endpoint_actions,
    hpa as hpa_actions,
    pod as pod_actions,
    workload as workload_actions,
)
from reslib.constants import QuantitySelectionModeEnum
from reslib.core.context import get_context, scenario_context, set_context
from reslib.k8s.exceptions import (
    CPUStressCommandFailed,
    EndpointDrainSelectionError,
    HpaScalePodReadyError,
    PodsSelectionError,
)


def _pod(name="pod-1", namespace="default", labels=None, pod_ip="10.0.0.1"):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace=namespace, labels=labels or {}),
        status=SimpleNamespace(pod_ip=pod_ip),
    )


def _scenario(
    *,
    namespace="default",
    workload="checkout",
    mode=QuantitySelectionModeEnum.ABSOLUTE,
    quantity=1,
    idle_cpu_pct=20,
    cpu_stress_threshold_pct=70,
    container_name="app",
    service_name="svc",
):
    return SimpleNamespace(
        template=SimpleNamespace(
            namespace=namespace,
            workload=workload,
            mode=mode,
            quantity=quantity,
            idle_cpu_pct=idle_cpu_pct,
            cpu_stress_threshold_pct=cpu_stress_threshold_pct,
            container_name=container_name,
            service_name=service_name,
        )
    )


def _workload(*, name="checkout", labels=None, ready_replicas=3, hpa=None):
    return SimpleNamespace(
        spec=SimpleNamespace(name=name, labels=labels or {"app": "checkout"}, hpa=hpa),
        runtime=SimpleNamespace(ready_replicas=ready_replicas),
    )


class _FakeStreamResponse:
    def __init__(self, *, stdout="", stderr=""):
        self._stdout = stdout
        self._stderr = stderr
        self.closed = False
        self.timeout = None

    def run_forever(self, timeout):
        self.timeout = timeout

    def read_channel(self, channel):
        return self._stdout if channel == hpa_actions.STDOUT_CHANNEL else self._stderr

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_drain_pod_from_service_endpoints_updates_labels_and_context(monkeypatch):
    fake_k8s = SimpleNamespace(patch_namespaced_pod=AsyncMock())
    selected_pod = _pod(
        name="pod-a",
        labels={"app": "checkout", "service-only": "blue"},
        pod_ip="10.2.0.5",
    )
    service = SimpleNamespace(
        metadata=SimpleNamespace(name="checkout-svc"),
        spec=SimpleNamespace(selector={"service-only": "blue"}),
    )

    monkeypatch.setattr(endpoint_actions, "KubernetesClient", lambda: fake_k8s)
    monkeypatch.setattr(
        endpoint_actions,
        "resolve_endpoint_drain_service",
        AsyncMock(return_value=service),
    )
    monkeypatch.setattr(
        endpoint_actions, "get_service_only_selector_key", lambda **_: "service-only"
    )
    monkeypatch.setattr(
        endpoint_actions,
        "get_ready_service_pods",
        AsyncMock(return_value=[selected_pod, _pod(name="pod-b", pod_ip="10.2.0.6")]),
    )
    monkeypatch.setattr(endpoint_actions.random, "choice", lambda pods: pods[0])
    monkeypatch.setattr(
        endpoint_actions.h, "utc_now_iso", lambda: "2026-07-07T12:00:00Z"
    )
    monkeypatch.setattr(endpoint_actions, "watch_until", AsyncMock(return_value=True))

    async with scenario_context(
        workload=_workload(labels={"app": "checkout"}),
        scenario=_scenario(namespace="payments"),
    ):
        result = await endpoint_actions.drain_pod_from_service_endpoints(
            timeout_seconds=45
        )

        drain_context = get_context("endpoint_drain")

    fake_k8s.patch_namespaced_pod.assert_awaited_once()
    assert drain_context["service_name"] == "checkout-svc"
    assert drain_context["pod_name"] == "pod-a"
    assert drain_context["drained_label_value"] == "resilops-drained-true"
    assert result["result"] == "endpoint_drained"
    assert result["observed"]["service_name"] == "checkout-svc"


@pytest.mark.asyncio
async def test_drain_pod_from_service_endpoints_requires_two_ready_pods(monkeypatch):
    monkeypatch.setattr(endpoint_actions, "KubernetesClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        endpoint_actions,
        "resolve_endpoint_drain_service",
        AsyncMock(
            return_value=SimpleNamespace(
                metadata=SimpleNamespace(name="checkout-svc"),
                spec=SimpleNamespace(selector={"service-only": "blue"}),
            )
        ),
    )
    monkeypatch.setattr(
        endpoint_actions, "get_service_only_selector_key", lambda **_: "service-only"
    )
    monkeypatch.setattr(
        endpoint_actions,
        "get_ready_service_pods",
        AsyncMock(return_value=[_pod(name="only-one")]),
    )

    async with scenario_context(
        workload=_workload(labels={"app": "checkout"}),
        scenario=_scenario(),
    ):
        with pytest.raises(EndpointDrainSelectionError, match="at least 2 Ready"):
            await endpoint_actions.drain_pod_from_service_endpoints()


@pytest.mark.asyncio
async def test_run_cpu_stress_returns_stdout_and_closes_stream(monkeypatch):
    fake_resp = _FakeStreamResponse(stdout="ok", stderr="")
    fake_api = SimpleNamespace(connect_get_namespaced_pod_exec=object())
    fake_k8s = SimpleNamespace(new_v1_api=lambda: fake_api)
    called = {}

    async def fake_to_thread(func, *args, **kwargs):
        called["last"] = (func, args, kwargs)
        return func(*args, **kwargs)

    monkeypatch.setattr(hpa_actions.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(hpa_actions.stream, "stream", lambda *args, **kwargs: fake_resp)
    monkeypatch.setattr(hpa_actions.h, "utc_now_iso", lambda: "2026-07-07T12:00:00Z")

    async with scenario_context():
        stdout, stderr = await hpa_actions.run_cpu_stress(
            k8s=fake_k8s,
            pod=_pod(name="stress-pod", namespace="payments"),
            cpu_percent=80,
            container_name="app",
            timeout=30,
        )

        assert get_context("stress_started_at") == "2026-07-07T12:00:00Z"

    assert stdout == "ok"
    assert stderr == ""
    assert fake_resp.timeout == 30
    assert fake_resp.closed is True
    assert called


@pytest.mark.asyncio
async def test_run_cpu_stress_raises_when_stderr_has_output(monkeypatch):
    fake_resp = _FakeStreamResponse(stdout="", stderr="boom")
    fake_api = SimpleNamespace(connect_get_namespaced_pod_exec=object())
    fake_k8s = SimpleNamespace(new_v1_api=lambda: fake_api)

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(hpa_actions.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(hpa_actions.stream, "stream", lambda *args, **kwargs: fake_resp)
    monkeypatch.setattr(hpa_actions.h, "utc_now_iso", lambda: "2026-07-07T12:00:00Z")

    async with scenario_context():
        with pytest.raises(CPUStressCommandFailed, match="CPU stress failed"):
            await hpa_actions.run_cpu_stress(
                k8s=fake_k8s,
                pod=_pod(name="stress-pod"),
                cpu_percent=80,
                container_name=None,
                timeout=30,
            )

    assert fake_resp.closed is True


@pytest.mark.asyncio
async def test_select_pods_to_stress_returns_empty_when_trigger_not_needed(monkeypatch):
    monkeypatch.setattr(
        hpa_actions, "get_hpa_resource_metric", lambda **_: SimpleNamespace(name="cpu")
    )
    monkeypatch.setattr(hpa_actions, "calculate_hpa_trigger", lambda **_: (0, 65))
    get_workload_pods = AsyncMock()
    monkeypatch.setattr(hpa_actions, "get_workload_pods", get_workload_pods)

    async with scenario_context(
        workload=_workload(hpa=SimpleNamespace()),
        scenario=_scenario(),
    ):
        pods, percent = await hpa_actions.select_pods_to_stress(k8s=SimpleNamespace())

    assert pods == []
    assert percent == 65
    get_workload_pods.assert_not_called()


@pytest.mark.asyncio
async def test_stress_cpu_hpa_returns_empty_when_no_pods_selected(monkeypatch):
    monkeypatch.setattr(hpa_actions, "KubernetesClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        hpa_actions, "select_pods_to_stress", AsyncMock(return_value=([], 75))
    )

    async with scenario_context(
        workload=_workload(hpa=SimpleNamespace()),
        scenario=_scenario(namespace="payments"),
    ):
        result = await hpa_actions.stress_cpu_hpa(max_stress_duration_seconds=60)

    assert result == {}


@pytest.mark.asyncio
async def test_stress_cpu_hpa_returns_context_on_scale_up(monkeypatch):
    async def fake_watch_task_group(*, tasks, **kwargs):
        for task, _ in tasks:
            task.close()
        raise HpaScalePodReadyError(
            error_code="SCALED",
            message="scaled",
            fix_hint="none",
        )

    async def fake_event_task(**kwargs):
        await asyncio.sleep(3600)

    monkeypatch.setattr(hpa_actions, "KubernetesClient", lambda: SimpleNamespace())
    monkeypatch.setattr(
        hpa_actions,
        "select_pods_to_stress",
        AsyncMock(return_value=([_pod(name="pod-a")], 80)),
    )
    monkeypatch.setattr(hpa_actions, "watch_task_group", fake_watch_task_group)
    monkeypatch.setattr(hpa_actions, "wait_for_hpa_scale_up_event", fake_event_task)
    monkeypatch.setattr(hpa_actions.h, "utc_now_iso", lambda: "2026-07-07T12:05:00Z")

    async with scenario_context(
        workload=_workload(name="checkout", hpa=SimpleNamespace()),
        scenario=_scenario(namespace="payments"),
    ):
        set_context("hpa_scaled_pods_ready", {"ready_replicas": 4})
        set_context("hpa_scale_up_event", {"event_name": "SuccessfulRescale"})
        set_context("stress_started_at", "2026-07-07T12:00:00Z")

        result = await hpa_actions.stress_cpu_hpa(max_stress_duration_seconds=60)
        stored = get_context("stress_context")

    assert result["result"] == "hpa_scale_up_detected"
    assert result["observed"]["ready_replicas"] == 4
    assert result["observed"]["event_name"] == "SuccessfulRescale"
    assert stored["stress_stopped_at"] == "2026-07-07T12:05:00Z"


def test_build_hpa_stress_tasks_creates_cpu_and_monitor_tasks():
    async def fake_watch_until(**kwargs):
        return kwargs

    original_watch_until = hpa_actions.watch_until
    hpa_actions.watch_until = fake_watch_until
    try:
        coro_tasks = []
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                hpa_actions,
                "run_cpu_stress",
                AsyncMock(return_value=("ok", "")),
            )

            async def _build():
                async with scenario_context(
                    workload=_workload(name="checkout", hpa=SimpleNamespace()),
                    scenario=_scenario(namespace="payments", container_name="web"),
                ):
                    tasks = hpa_actions.build_hpa_stress_tasks(
                        k8s=SimpleNamespace(),
                        pods_to_stress=[_pod(name="pod-a"), _pod(name="pod-b")],
                        stress_cpu_percent=85,
                        args=SimpleNamespace(max_stress_duration_seconds=90),
                    )
                    return tasks

            tasks = asyncio.run(_build())
            coro_tasks = [task for task, _ in tasks]

            assert len(tasks) == 4
            assert tasks[0][1] == "action:stress:pod:cpu:pod-a"
            assert tasks[1][1] == "action:stress:pod:cpu:pod-b"
            assert tasks[2][1] == "monitor:hpa:scale:pod:ready"
            assert tasks[3][1] == "monitor:container:crash"
    finally:
        hpa_actions.watch_until = original_watch_until
        for coro in coro_tasks:
            coro.close()


@pytest.mark.asyncio
async def test_pod_absent_inverts_pod_exists(monkeypatch):
    monkeypatch.setattr(pod_actions, "pod_exists", AsyncMock(return_value=False))

    assert await pod_actions.pod_absent() is True


@pytest.mark.asyncio
async def test_build_pod_deletion_task_deletes_pod_and_returns_watch(monkeypatch):
    fake_k8s = SimpleNamespace(delete_namespaced_pod=AsyncMock())

    async def fake_watch_until(**kwargs):
        return kwargs

    monkeypatch.setattr(pod_actions, "watch_until", fake_watch_until)
    monkeypatch.setattr(pod_actions.h, "utc_now_iso", lambda: "2026-07-07T12:00:00Z")

    async with scenario_context():
        task, name = await pod_actions.build_pod_deletion_task(
            k8s=fake_k8s,
            pod=_pod(name="pod-a"),
            namespace="payments",
            timeout=30,
        )
        watched = await task
        assert get_context("last_pod_killed_at") == "2026-07-07T12:00:00Z"

    assert name == "delete:pod:pod-a"
    assert watched["namespace"] == "payments"
    fake_k8s.delete_namespaced_pod.assert_awaited_once()


@pytest.mark.asyncio
async def test_build_pod_eviction_task_evicts_pod_and_returns_watch(monkeypatch):
    fake_k8s = SimpleNamespace(create_namespaced_pod_eviction=AsyncMock())

    async def fake_watch_until(**kwargs):
        return kwargs

    monkeypatch.setattr(pod_actions, "watch_until", fake_watch_until)
    monkeypatch.setattr(pod_actions.h, "utc_now_iso", lambda: "2026-07-07T12:00:00Z")

    async with scenario_context():
        task, name = await pod_actions.build_pod_eviction_task(
            k8s=fake_k8s,
            pod=_pod(name="pod-a"),
            namespace="payments",
            timeout=30,
        )
        watched = await task
        assert get_context("last_pod_evicted_at") == "2026-07-07T12:00:00Z"

    assert name == "evict:pod:pod-a"
    assert watched["namespace"] == "payments"
    fake_k8s.create_namespaced_pod_eviction.assert_awaited_once()


@pytest.mark.asyncio
async def test_terminate_pods_raises_when_selection_resolves_to_zero():
    async with scenario_context(
        workload=_workload(ready_replicas=1),
        scenario=_scenario(mode=QuantitySelectionModeEnum.PERCENTAGE, quantity=1),
    ):
        with pytest.raises(PodsSelectionError, match="resolved to 0 pods"):
            await pod_actions.terminate_pods()


@pytest.mark.asyncio
async def test_terminate_pods_returns_summary(monkeypatch):
    fake_k8s = SimpleNamespace()
    monkeypatch.setattr(pod_actions, "KubernetesClient", lambda: fake_k8s)
    monkeypatch.setattr(
        pod_actions,
        "get_workload_pods",
        AsyncMock(return_value=[_pod(name="pod-a"), _pod(name="pod-b")]),
    )
    monkeypatch.setattr(pod_actions.random, "sample", lambda pods, k: pods[:k])
    monkeypatch.setattr(
        pod_actions, "get_pod_termination_timeout", lambda pods, max_timeout: 45
    )
    monkeypatch.setattr(
        pod_actions,
        "build_pod_deletion_task",
        AsyncMock(return_value=("delete-task", "delete:pod:pod-a")),
    )
    monkeypatch.setattr(pod_actions, "watch_task_group", AsyncMock(return_value=None))

    async with scenario_context(
        workload=_workload(ready_replicas=3),
        scenario=_scenario(namespace="payments", quantity=1),
        last_pod_killed_at="2026-07-07T12:00:00Z",
    ):
        result = await pod_actions.terminate_pods(timeout_seconds=60)

    assert result["result"] == "pods_terminated"
    assert result["observed"]["terminated_pods"] == 1
    assert result["observed"]["termination_timeout_seconds"] == 45


@pytest.mark.asyncio
async def test_evict_pods_returns_summary(monkeypatch):
    fake_k8s = SimpleNamespace()
    monkeypatch.setattr(pod_actions, "KubernetesClient", lambda: fake_k8s)
    monkeypatch.setattr(
        pod_actions,
        "get_workload_pods",
        AsyncMock(return_value=[_pod(name="pod-a"), _pod(name="pod-b")]),
    )
    monkeypatch.setattr(pod_actions.random, "sample", lambda pods, k: pods[:k])
    monkeypatch.setattr(
        pod_actions, "get_pod_termination_timeout", lambda pods, max_timeout: 40
    )
    monkeypatch.setattr(
        pod_actions,
        "build_pod_eviction_task",
        AsyncMock(return_value=("evict-task", "evict:pod:pod-a")),
    )
    monkeypatch.setattr(pod_actions, "watch_task_group", AsyncMock(return_value=None))

    async with scenario_context(
        workload=_workload(ready_replicas=3),
        scenario=_scenario(namespace="payments", quantity=1),
        last_pod_evicted_at="2026-07-07T12:01:00Z",
    ):
        result = await pod_actions.evict_pods(timeout_seconds=60)

    assert result["result"] == "pods_evicted"
    assert result["observed"]["evicted_pods"] == 1
    assert result["observed"]["eviction_timeout_seconds"] == 40


@pytest.mark.asyncio
async def test_perform_rolling_restart_patches_deployment_and_records_context(
    monkeypatch,
):
    fake_deployment = SimpleNamespace(metadata=SimpleNamespace(generation=7))
    fake_k8s = SimpleNamespace(
        patch_namespaced_deployment=AsyncMock(return_value=fake_deployment)
    )
    monkeypatch.setattr(workload_actions, "KubernetesClient", lambda: fake_k8s)
    monkeypatch.setattr(
        workload_actions.h, "utc_now_iso", lambda: "2026-07-07T12:00:00Z"
    )

    async with scenario_context(
        workload=_workload(name="checkout"),
        scenario=_scenario(namespace="payments", workload="checkout"),
    ):
        result = await workload_actions.perform_rolling_restart()
        started_at = get_context("rolling_restart_started_at")
        generation = get_context("rolling_restart_generation")

    assert result["result"] == "rolling_restart_started"
    assert result["observed"]["workload"] == "checkout"
    assert started_at == "2026-07-07T12:00:00Z"
    assert generation == 7
    fake_k8s.patch_namespaced_deployment.assert_awaited_once()
