from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kubernetes.client.exceptions import ApiException

from reslib.core.context import get_context, scenario_context
from reslib.k8s import workloads as workload_helpers
from reslib.k8s.exceptions import (
    RollingRestartCompleteError,
    WorkloadFaultyError,
    WorkloadNotFound,
)


class HashableNamespace(SimpleNamespace):
    __hash__ = object.__hash__


def _condition(
    cond_type,
    status,
    *,
    reason=None,
    message=None,
    at=None,
):
    return SimpleNamespace(
        type=cond_type,
        status=status,
        reason=reason,
        message=message,
        last_transition_time=at,
    )


def _probe(*, path="/health", port=8080, host=None, scheme="HTTP"):
    return SimpleNamespace(
        http_get=SimpleNamespace(path=path, port=port, host=host, scheme=scheme)
    )


def _service(*, name="checkout", selector=None, ports=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(selector=selector, ports=ports or []),
    )


def _deployment(
    *,
    name="checkout",
    namespace="payments",
    replicas=3,
    ready_replicas=3,
    updated_replicas=3,
    available_replicas=3,
    unavailable_replicas=0,
    observed_generation=2,
    generation=2,
    conditions=None,
    template_labels=None,
    selector_labels=None,
    containers=None,
):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, namespace=namespace, generation=generation),
        spec=SimpleNamespace(
            replicas=replicas,
            selector=SimpleNamespace(match_labels=selector_labels or {"app": name}),
            template=SimpleNamespace(
                metadata=SimpleNamespace(labels=template_labels or {"app": name}),
                spec=SimpleNamespace(containers=containers or []),
            ),
        ),
        status=SimpleNamespace(
            ready_replicas=ready_replicas,
            replicas=replicas,
            updated_replicas=updated_replicas,
            available_replicas=available_replicas,
            unavailable_replicas=unavailable_replicas,
            observed_generation=observed_generation,
            conditions=conditions or [],
        ),
    )


def _container(name="app", resources=None, readiness=None, liveness=None, startup=None):
    return SimpleNamespace(
        name=name,
        resources=resources,
        readiness_probe=readiness,
        liveness_probe=liveness,
        startup_probe=startup,
    )


def _snapshot(hpa=None, pdb=None):
    return SimpleNamespace(
        get_hpa=lambda deployment: hpa,
        get_pdb=lambda labels: pdb,
    )


def _hpa(name="checkout-hpa"):
    metric = SimpleNamespace(
        type="Resource",
        resource=SimpleNamespace(
            to_dict=lambda: {"name": "cpu", "target": {"average_utilization": 60}}
        ),
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(
            min_replicas=2,
            max_replicas=5,
            metrics=[metric],
            scale_target_ref=SimpleNamespace(name="checkout"),
        ),
    )


def _pdb(min_available=2, max_unavailable=1):
    return SimpleNamespace(
        spec=SimpleNamespace(
            min_available=min_available,
            max_unavailable=max_unavailable,
            selector=SimpleNamespace(match_labels={"app": "checkout"}),
        )
    )


def test_deployment_condition_helpers_cover_progress_available_and_faulty():
    now = datetime.now(timezone.utc)
    deployment = _deployment(
        conditions=[
            _condition("Available", "True", reason="MinimumReplicasAvailable", at=now),
            _condition("Progressing", "True", reason="ReplicaSetUpdated", at=now),
        ]
    )

    conditions = workload_helpers.get_deployment_conditions(deployment)

    assert len(conditions) == 2
    assert workload_helpers.is_deployment_in_progress(deployment) is True
    assert workload_helpers.is_deployment_available(deployment) is True
    assert workload_helpers.is_deployment_faulty(deployment) is False

    faulty = _deployment(
        conditions=[
            _condition(
                "Progressing",
                "False",
                reason="ProgressDeadlineExceeded",
            )
        ]
    )
    assert workload_helpers.is_deployment_faulty(faulty) is True


@pytest.mark.asyncio
async def test_raise_on_rolling_restart_complete_handles_faulty_pending_and_success(
    monkeypatch,
):
    started_at = datetime.now(timezone.utc)
    ready_time = started_at + timedelta(seconds=10)

    faulty = _deployment(
        conditions=[
            _condition("Progressing", "False", reason="ProgressDeadlineExceeded")
        ]
    )
    fake_k8s = SimpleNamespace(
        read_namespaced_deployment=AsyncMock(return_value=faulty)
    )
    with pytest.raises(WorkloadFaultyError, match="failed while rolling restart"):
        await workload_helpers.raise_on_rolling_restart_complete(
            k8s=fake_k8s,
            workload_name="checkout",
            namespace="payments",
            started_at=started_at.isoformat(),
            target_generation=2,
        )

    not_ready = _deployment(
        ready_replicas=2,
        updated_replicas=2,
        available_replicas=2,
        unavailable_replicas=1,
        observed_generation=1,
        generation=2,
        conditions=[_condition("Available", "True", reason="MinimumReplicasAvailable")],
    )
    fake_k8s.read_namespaced_deployment = AsyncMock(return_value=not_ready)
    assert (
        await workload_helpers.raise_on_rolling_restart_complete(
            k8s=fake_k8s,
            workload_name="checkout",
            namespace="payments",
            started_at=started_at.isoformat(),
            target_generation=2,
        )
        is None
    )

    complete = _deployment(
        conditions=[_condition("Available", "True", reason="MinimumReplicasAvailable")]
    )
    fake_k8s.read_namespaced_deployment = AsyncMock(return_value=complete)
    monkeypatch.setattr(
        workload_helpers,
        "get_pods_by_labels",
        AsyncMock(return_value=["pod-a", "pod-b"]),
    )
    monkeypatch.setattr(
        workload_helpers,
        "get_latest_pod_ready_time",
        lambda pods: ready_time,
    )

    async with scenario_context():
        with pytest.raises(
            RollingRestartCompleteError, match="completed rolling restart"
        ):
            await workload_helpers.raise_on_rolling_restart_complete(
                k8s=fake_k8s,
                workload_name="checkout",
                namespace="payments",
                started_at=started_at.isoformat(),
                target_generation=2,
            )

        observed = get_context("rolling_restart_complete")

    assert observed["ready_replicas"] == 3
    assert observed["latest_pod_ready_time"] == ready_time.isoformat()


@pytest.mark.asyncio
async def test_get_namespace_policies_snapshot_builds_and_caches_results():
    workload_helpers._namespace_cache.clear()
    fake_hpa = _hpa()
    fake_pdb = _pdb()
    fake_k8s = HashableNamespace(
        list_namespaced_horizontal_pod_autoscaler=AsyncMock(
            return_value=SimpleNamespace(items=[fake_hpa])
        ),
        list_namespaced_pod_disruption_budget=AsyncMock(
            return_value=SimpleNamespace(items=[fake_pdb])
        ),
    )

    first = await workload_helpers.get_namespace_policies_snapshot(
        k8s=fake_k8s,
        namespace="payments",
    )
    second = await workload_helpers.get_namespace_policies_snapshot(
        k8s=fake_k8s,
        namespace="payments",
    )

    assert first is second
    assert first.hpas["checkout"].metadata.name == "checkout-hpa"
    assert first.pdbs == [fake_pdb]
    fake_k8s.list_namespaced_horizontal_pod_autoscaler.assert_awaited_once()


def test_probe_and_container_builders_normalize_service_endpoints():
    service = _service(
        name="checkout",
        selector={"app": "checkout"},
        ports=[SimpleNamespace(port=80, target_port=8080)],
    )
    probe = _probe(port=8080)
    deployment = _deployment(
        containers=[
            _container(
                resources=SimpleNamespace(
                    requests={"cpu": "100m"},
                    limits={"cpu": "500m"},
                ),
                readiness=probe,
                liveness=probe,
                startup=probe,
            )
        ]
    )

    assert workload_helpers._resolve_service_port_for_probe(8080, service) == 80
    http_get = workload_helpers._build_probe_http_get(probe, service, "payments")
    assert http_get.host == "checkout.payments.svc.cluster.local"
    assert http_get.port == 80
    assert http_get.scheme == "http"

    containers = workload_helpers._build_container_specs(
        deployment,
        service,
        include_resources=True,
    )
    assert containers[0].resources.requests["cpu"] == "100m"
    assert containers[0].health.startup.port == 80

    containers = workload_helpers._build_container_specs(
        deployment,
        service,
        include_resources=False,
    )
    assert containers[0].resources is None
    assert containers[0].health.startup is None
    assert workload_helpers._resolve_service_port_for_probe(9090, None) == 9090
    assert workload_helpers._build_probe_http_get(None, service, "payments") is None


def test_hpa_service_spec_policy_and_runtime_builders_cover_common_paths():
    deployment = _deployment(
        conditions=[_condition("Available", "True", reason="MinimumReplicasAvailable")]
    )
    service_a = _service(name="z-service", selector={"app": "checkout"})
    service_b = _service(name="a-service", selector={"app": "checkout"})
    snapshot = _snapshot(hpa=_hpa(), pdb=_pdb())

    service = workload_helpers.get_service(deployment, [service_a, service_b])
    spec = workload_helpers.get_workload_spec(
        deployment=deployment,
        snapshot=snapshot,
        services=[service_a, service_b],
        is_full=True,
    )
    policies = workload_helpers.get_workload_policies(
        snapshot=snapshot, deployment=deployment
    )
    runtime = workload_helpers.get_workload_runtime(deployment)
    minimal_runtime = workload_helpers.get_workload_runtime(
        _deployment(conditions=[]),
        is_full=False,
    )
    reconciling_runtime = workload_helpers.get_workload_runtime(
        _deployment(
            conditions=[_condition("Progressing", "True", reason="ReplicaSetUpdated")]
        )
    )
    degraded_runtime = workload_helpers.get_workload_runtime(
        _deployment(
            conditions=[
                _condition("Progressing", "False", reason="ProgressDeadlineExceeded")
            ]
        )
    )
    no_hpa_spec = workload_helpers.get_workload_spec(
        deployment=deployment,
        snapshot=_snapshot(hpa=None, pdb=None),
        services=None,
        is_full=True,
    )

    assert service.metadata.name == "a-service"
    assert workload_helpers.get_service(deployment, None) is None
    assert (
        workload_helpers.get_service(
            deployment,
            [_service(name="mismatch", selector={"app": "other"})],
        )
        is None
    )
    assert spec.service_name == "a-service"
    assert spec.hpa.name == "checkout-hpa"
    assert spec.labels == {"app": "checkout"}
    assert no_hpa_spec.hpa is None
    assert policies.pdb.min_available == 2
    assert runtime.status.value == "healthy"
    assert minimal_runtime.conditions is None
    assert reconciling_runtime.status.value == "reconciling"
    assert degraded_runtime.status.value == "degraded"


@pytest.mark.asyncio
async def test_get_workload_handles_not_found_and_success(monkeypatch):
    workload_helpers._namespace_cache.clear()
    deployment = _deployment(
        conditions=[_condition("Available", "True", reason="MinimumReplicasAvailable")]
    )
    fake_k8s = HashableNamespace(
        list_namespaced_horizontal_pod_autoscaler=AsyncMock(
            return_value=SimpleNamespace(items=[_hpa()])
        ),
        list_namespaced_pod_disruption_budget=AsyncMock(
            return_value=SimpleNamespace(items=[_pdb()])
        ),
        list_namespaced_service=AsyncMock(
            return_value=SimpleNamespace(
                items=[_service(name="checkout", selector={"app": "checkout"})]
            )
        ),
        read_namespaced_deployment=AsyncMock(
            side_effect=ApiException(status=404, reason="not found")
        ),
    )

    with pytest.raises(WorkloadNotFound, match="was not found in namespace"):
        await workload_helpers.get_workload(
            namespace="payments",
            name="checkout",
            k8s=fake_k8s,
        )

    fake_k8s.read_namespaced_deployment = AsyncMock(return_value=deployment)
    workload = await workload_helpers.get_workload(
        namespace="payments",
        name="checkout",
        k8s=fake_k8s,
    )

    assert workload.spec.name == "checkout"
    assert workload.spec.service_name == "checkout"
    assert workload.policies.pdb.max_unavailable == 1
    assert workload.runtime.ready_replicas == 3

    fake_k8s.read_namespaced_deployment = AsyncMock(
        side_effect=ApiException(status=500, reason="server error")
    )
    with pytest.raises(ApiException):
        await workload_helpers.get_workload(
            namespace="payments",
            name="checkout",
            k8s=fake_k8s,
        )
