import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from reslib.core.context import get_context, scenario_context, set_context
from reslib.k8s.exceptions import (
    ReachedDesiredReplicaError,
    ReplicasRestoredError,
    RollingRestartCompleteError,
)
from reslib.rollbacks import (
    endpoint as endpoint_rollbacks,
    hpa as hpa_rollbacks,
    pod as pod_rollbacks,
    workload as workload_rollbacks,
)


def _scenario(*, namespace="default"):
    return SimpleNamespace(template=SimpleNamespace(namespace=namespace))


def _workload(*, name="checkout"):
    return SimpleNamespace(spec=SimpleNamespace(name=name))


@pytest.mark.asyncio
async def test_restore_pod_to_service_endpoints_restores_label_and_waits(monkeypatch):
    fake_k8s = SimpleNamespace(patch_namespaced_pod=AsyncMock())
    monkeypatch.setattr(endpoint_rollbacks, "KubernetesClient", lambda: fake_k8s)
    monkeypatch.setattr(endpoint_rollbacks, "watch_until", AsyncMock(return_value=True))
    monkeypatch.setattr(
        endpoint_rollbacks.h, "utc_now_iso", lambda: "2026-07-07T12:10:00Z"
    )

    async with scenario_context(
        endpoint_drain={
            "namespace": "payments",
            "service_name": "checkout-svc",
            "pod_name": "pod-a",
            "pod_ip": "10.2.0.5",
            "label_key": "service-only",
            "original_label_value": "blue",
        }
    ):
        result = await endpoint_rollbacks.restore_pod_to_service_endpoints(
            timeout_seconds=45
        )

    assert result["result"] == "endpoint_restored"
    assert result["observed"]["pod_name"] == "pod-a"
    fake_k8s.patch_namespaced_pod.assert_awaited_once()


@pytest.mark.asyncio
async def test_wait_for_hpa_scale_down_returns_success_context(monkeypatch):
    async def fake_watch_task_group(*, tasks, **kwargs):
        for task, _ in tasks:
            task.close()
        raise ReplicasRestoredError(
            error_code="RESTORED",
            message="restored",
            fix_hint="none",
        )

    async def fake_event_task(**kwargs):
        await asyncio.sleep(3600)

    monkeypatch.setattr(hpa_rollbacks, "KubernetesClient", lambda: SimpleNamespace())
    monkeypatch.setattr(hpa_rollbacks, "watch_task_group", fake_watch_task_group)
    monkeypatch.setattr(hpa_rollbacks, "wait_for_hpa_scale_down_event", fake_event_task)

    async with scenario_context(
        workload=SimpleNamespace(spec=SimpleNamespace(name="checkout")),
        scenario=_scenario(namespace="payments"),
        stress_context={"ready_replicas": 4},
    ):
        set_context("replicas_restored", {"ready_replicas": 2})
        set_context("hpa_scale_down_event", {"event_name": "ScaleDown"})

        result = await hpa_rollbacks.wait_for_hpa_scale_down(timeout_seconds=90)

    assert result["result"] == "hpa_scale_down_stabilized"
    assert result["observed"]["ready_replicas"] == 2
    assert result["observed"]["event_name"] == "ScaleDown"


@pytest.mark.asyncio
async def test_wait_until_pod_respawn_returns_context_when_replicas_recover(
    monkeypatch,
):
    async def fake_watch_task_group(*, tasks, **kwargs):
        for task, _ in tasks:
            task.close()
        raise ReachedDesiredReplicaError(
            error_code="DESIRED",
            message="desired replicas reached",
            fix_hint="none",
        )

    monkeypatch.setattr(pod_rollbacks, "KubernetesClient", lambda: SimpleNamespace())
    monkeypatch.setattr(pod_rollbacks, "watch_task_group", fake_watch_task_group)

    async with scenario_context(
        workload=_workload(name="checkout"),
        scenario=_scenario(namespace="payments"),
        last_pod_killed_at="2026-07-07T12:00:00Z",
    ):
        set_context("desired_replica_reached", {"ready_replicas": 3})
        result = await pod_rollbacks.wait_until_pod_respawn(timeout_seconds=60)

    assert result["result"] == "pods_respawned"
    assert result["observed"]["ready_replicas"] == 3


@pytest.mark.asyncio
async def test_wait_until_rolling_restart_complete_returns_context(monkeypatch):
    async def fake_watch_task_group(*, tasks, **kwargs):
        for task, _ in tasks:
            task.close()
        raise RollingRestartCompleteError(
            error_code="DONE",
            message="restart complete",
            fix_hint="none",
        )

    monkeypatch.setattr(
        workload_rollbacks, "KubernetesClient", lambda: SimpleNamespace()
    )
    monkeypatch.setattr(workload_rollbacks, "watch_task_group", fake_watch_task_group)

    async with scenario_context(
        workload=_workload(name="checkout"),
        scenario=_scenario(namespace="payments"),
        rolling_restart_started_at="2026-07-07T12:00:00Z",
        rolling_restart_generation=7,
        rolling_restart_complete={"ready_replicas": 3, "generation": 7},
    ):
        result = await workload_rollbacks.wait_until_rolling_restart_complete(
            timeout_seconds=60
        )
        observed = get_context("rolling_restart_complete")

    assert result["result"] == "rolling_restart_completed"
    assert result["observed"] == observed
