from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from reslib.constants import K8DeploymentKind, WorkloadStatusEnum
from reslib.k8s import (
    discovery as discovery_helpers,
    endpoints as endpoint_helpers,
    snapshot as snapshot_helpers,
)
from reslib.k8s.exceptions import EndpointDrainSelectionError
from reslib.k8s.schema import DiscoveryNamespaceConfigSchema


def _pod(name="pod-a", labels=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, labels=labels or {}),
        status=SimpleNamespace(),
    )


def _service(*, name="svc", selector=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        spec=SimpleNamespace(selector=selector),
    )


def test_discover_workloads_yields_workload_state(monkeypatch):
    deployments = [SimpleNamespace(metadata=SimpleNamespace(name="checkout"))]
    services = [_service(name="checkout-svc", selector={"app": "checkout"})]
    fake_k8s = SimpleNamespace(
        apps=SimpleNamespace(
            list_namespaced_deployment=lambda namespace: SimpleNamespace(
                items=deployments
            )
        ),
        v1_api=SimpleNamespace(
            list_namespaced_service=lambda namespace: SimpleNamespace(items=services)
        ),
    )
    monkeypatch.setattr(
        discovery_helpers,
        "get_workload_spec",
        lambda **_: {
            "name": "checkout",
            "kind": K8DeploymentKind.DEPLOYMENT,
            "replicas": 2,
        },
    )
    monkeypatch.setattr(
        discovery_helpers,
        "get_workload_runtime",
        lambda *args, **kwargs: {
            "ready_replicas": 2,
            "status": WorkloadStatusEnum.healthy,
        },
    )

    workloads = list(discovery_helpers.discover_workloads(fake_k8s, "payments"))

    assert len(workloads) == 1
    assert workloads[0].spec.name == "checkout"
    assert workloads[0].runtime.ready_replicas == 2


def test_discover_namespaces_builds_namespace_states(monkeypatch):
    monkeypatch.setattr(
        discovery_helpers, "KubernetesClient", lambda: SimpleNamespace()
    )
    monkeypatch.setattr(
        discovery_helpers,
        "discover_workloads",
        lambda k8s, namespace: [
            SimpleNamespace(spec=SimpleNamespace(name=f"{namespace}-app"), runtime=None)
        ],
    )

    states = discovery_helpers.discover_namespaces(
        DiscoveryNamespaceConfigSchema(namespaces=["payments", "orders"])
    )

    assert [state.name for state in states] == ["payments", "orders"]
    assert states[0].workloads[0].spec.name == "payments-app"
    assert states[1].workloads[0].spec.name == "orders-app"


def test_namespace_snapshot_returns_matching_hpa_and_pdb():
    deployment = SimpleNamespace(metadata=SimpleNamespace(name="checkout"))
    matching_pdb = SimpleNamespace(
        spec=SimpleNamespace(selector=SimpleNamespace(match_labels={"app": "checkout"}))
    )
    other_pdb = SimpleNamespace(
        spec=SimpleNamespace(selector=SimpleNamespace(match_labels={"app": "other"}))
    )
    snapshot = snapshot_helpers.NamespaceSnapshot(
        hpas={"checkout": "hpa-obj"},
        pdbs=[other_pdb, matching_pdb],
    )

    assert snapshot.get_hpa(deployment) == "hpa-obj"
    assert snapshot.get_pdb({"app": "checkout", "tier": "web"}) is matching_pdb
    assert snapshot.get_pdb({"app": "missing"}) is None


def test_labels_to_selector_and_service_only_selector_key():
    assert endpoint_helpers.labels_to_selector({"app": "checkout", "tier": "web"}) in {
        "app=checkout,tier=web",
        "tier=web,app=checkout",
    }
    assert (
        endpoint_helpers.get_service_only_selector_key(
            service_selector={"app": "checkout", "service-only": "blue"},
            workload_selector={"app": "checkout"},
        )
        == "service-only"
    )


def test_get_service_only_selector_key_raises_when_no_distinct_label():
    with pytest.raises(
        EndpointDrainSelectionError, match="Service selector does not contain"
    ):
        endpoint_helpers.get_service_only_selector_key(
            service_selector={"app": "checkout"},
            workload_selector={"app": "checkout"},
        )


@pytest.mark.asyncio
async def test_resolve_endpoint_drain_service_handles_all_paths():
    fake_k8s = SimpleNamespace(
        read_namespaced_service=AsyncMock(
            return_value=_service(name="checkout-svc", selector={"app": "checkout"})
        )
    )
    workload = SimpleNamespace(spec=SimpleNamespace(name="checkout", service_name=None))

    with pytest.raises(
        EndpointDrainSelectionError, match="does not have a resolved Service"
    ):
        await endpoint_helpers.resolve_endpoint_drain_service(
            k8s=fake_k8s,
            namespace="payments",
            workload=workload,
            service_name=None,
        )

    fake_k8s.read_namespaced_service = AsyncMock(
        return_value=_service(name="checkout-svc", selector=None)
    )
    with pytest.raises(EndpointDrainSelectionError, match="does not have a selector"):
        await endpoint_helpers.resolve_endpoint_drain_service(
            k8s=fake_k8s,
            namespace="payments",
            workload=SimpleNamespace(
                spec=SimpleNamespace(name="checkout", service_name="checkout-svc")
            ),
        )

    fake_k8s.read_namespaced_service = AsyncMock(
        return_value=_service(name="checkout-svc", selector={"app": "checkout"})
    )
    service = await endpoint_helpers.resolve_endpoint_drain_service(
        k8s=fake_k8s,
        namespace="payments",
        workload=SimpleNamespace(
            spec=SimpleNamespace(name="checkout", service_name="checkout-svc")
        ),
    )

    assert service.metadata.name == "checkout-svc"


@pytest.mark.asyncio
async def test_get_ready_service_pods_filters_by_readiness(monkeypatch):
    pods = [_pod(name="pod-a"), _pod(name="pod-b")]
    monkeypatch.setattr(
        endpoint_helpers, "get_pods_by_labels", AsyncMock(return_value=pods)
    )
    monkeypatch.setattr(
        endpoint_helpers,
        "is_pod_ready",
        lambda pod: pod.metadata.name == "pod-b",
    )

    ready = await endpoint_helpers.get_ready_service_pods(
        k8s=SimpleNamespace(),
        namespace="payments",
        service_selector={"app": "checkout"},
    )

    assert [pod.metadata.name for pod in ready] == ["pod-b"]


@pytest.mark.asyncio
async def test_endpoint_slice_contains_ip_and_wrappers():
    slices = SimpleNamespace(
        items=[
            SimpleNamespace(
                endpoints=[
                    SimpleNamespace(addresses=["10.0.0.1"]),
                    SimpleNamespace(addresses=[]),
                ]
            ),
            SimpleNamespace(endpoints=[SimpleNamespace(addresses=["10.0.0.2"])]),
        ]
    )
    fake_k8s = SimpleNamespace(
        list_namespaced_endpoint_slice=AsyncMock(return_value=slices)
    )

    assert (
        await endpoint_helpers.endpoint_slice_contains_ip(
            k8s=fake_k8s,
            namespace="payments",
            service_name="checkout-svc",
            pod_ip="10.0.0.2",
        )
        is True
    )
    assert (
        await endpoint_helpers.endpoint_slice_contains_ip(
            k8s=fake_k8s,
            namespace="payments",
            service_name="checkout-svc",
            pod_ip="10.0.0.9",
        )
        is False
    )

    assert (
        await endpoint_helpers.pod_ip_present_in_endpoint_slices(
            k8s=fake_k8s,
            namespace="payments",
            service_name="checkout-svc",
            pod_ip="10.0.0.1",
        )
        is True
    )
    assert (
        await endpoint_helpers.pod_ip_absent_from_endpoint_slices(
            k8s=fake_k8s,
            namespace="payments",
            service_name="checkout-svc",
            pod_ip="10.0.0.9",
        )
        is True
    )
