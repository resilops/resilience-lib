from typing import Generator

from kubernetes.client import V1Deployment

from reslib.k8s.client import KubernetesClient
from reslib.k8s.schema import (
    AgentConfigSchema,
    ClusterState,
    NamespaceState,
    WorkloadState,
)
from reslib.k8s.utils import (
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
    snapshot = get_namespace_snapshot(k8s=k8s_client, namespace=namespace)

    deployments: list[V1Deployment] = k8s_client.apps.list_namespaced_deployment(
        namespace=namespace
    ).items

    for deployment in deployments:
        yield WorkloadState(
            spec=get_workload_spec(snapshot=snapshot, deployment=deployment),
            policies=get_workload_policies(snapshot=snapshot, deployment=deployment),
            status=get_workload_status(deployment),
        )


def discover_cluster(
    k8s_client: KubernetesClient, agent_config: AgentConfigSchema
) -> ClusterState:
    """
    Discover all namespaces in the cluster and their workloads.

    This function performs a full traversal of the cluster:
    namespaces → workloads.

    Args:
        k8s_client: Kubernetes client instance.
        agent_config: Agent config schema.

    Yields:
        NamespaceState objects containing workloads for each namespace.
    """
    cluster_state = ClusterState(cluster_id=agent_config.cluster_id)

    for ns_name in agent_config.namespaces:
        ns_state = NamespaceState(name=ns_name)

        for workload in discover_workloads(k8s_client, ns_name):
            ns_state.workloads[workload.spec.name] = workload

        cluster_state.namespaces[ns_state.name] = ns_state

    return cluster_state
