from typing import Generator

from kubernetes.client import V1Deployment

from reslib.k8s.client import KubernetesClient
from reslib.k8s.schema import (
    ClusterState,
    NamespaceState,
    WorkloadState,
)
from reslib.k8s.utils import (
    current_cluster_name,
    get_namespace_snapshot,
    get_workload_policies,
    get_workload_spec,
    get_workload_status,
)


def discover_workloads(
    k8s_client: KubernetesClient,
    namespace: str,
) -> Generator[WorkloadState, None, None]:
    """
    Discover all workloads (Deployments) in a namespace.

    This function is intended for *bulk discovery*. It builds namespace-wide
    indexes for HPAs and PodDisruptionBudgets once, then applies them to
    each Deployment in the namespace.

    Args:
        k8s_client: Kubernetes client instance.
        namespace: Namespace to search in.

    Yields:
        WorkloadState objects representing each discovered Deployment.
    """
    snapshot = get_namespace_snapshot(namespace=namespace)

    deployments: list[V1Deployment] = k8s_client.apps.list_namespaced_deployment(
        namespace=namespace
    ).items

    for deployment in deployments:
        yield WorkloadState(
            spec=get_workload_spec(snapshot=snapshot, deployment=deployment),
            policies=get_workload_policies(snapshot=snapshot, deployment=deployment),
            status=get_workload_status(deployment),
        )


def discover_namespaces(
    k8s_client: KubernetesClient,
) -> Generator[NamespaceState, None, None]:
    """
    Discover all namespaces in the cluster and their workloads.

    This function performs a full traversal of the cluster:
    namespaces → workloads.

    Args:
        k8s_client: Kubernetes client instance.

    Yields:
        NamespaceState objects containing workloads for each namespace.
    """
    namespaces = k8s_client.v1_api.list_namespace().items

    for ns in namespaces:
        ns_state = NamespaceState(
            name=ns.metadata.name,
            labels=ns.metadata.labels or {},
        )

        for workload in discover_workloads(k8s_client, ns.metadata.name):
            ns_state.workloads[workload.spec.name] = workload

        yield ns_state


def discover_cluster(k8s_client: KubernetesClient) -> ClusterState:
    """
    Discover the complete cluster state.

    This is the top-level discovery entrypoint and should be used when a
    full, consistent snapshot of the cluster is required.

    Args:
        k8s_client: Kubernetes client instance.

    Returns:
        ClusterState representing all namespaces and workloads in the cluster.
    """
    cluster_state = ClusterState(name=current_cluster_name())

    for ns_state in discover_namespaces(k8s_client):
        cluster_state.namespaces[ns_state.name] = ns_state

    return cluster_state
