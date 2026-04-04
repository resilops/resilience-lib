from typing import Generator, List

from kubernetes.client import V1Deployment

from reslib.k8s.client import KubernetesClient
from reslib.k8s.schema import (
    DiscoveryNamespaceConfigSchema,
    NamespaceState,
    WorkloadState,
)
from reslib.k8s.utils import get_workload_runtime, get_workload_spec


def discover_workloads(
    k8s: KubernetesClient,
    namespace: str,
) -> Generator[WorkloadState, None, None]:
    """
    Discover all workloads (Deployments) in a namespace.

    This function is intended for *bulk discovery*. It builds namespace-wide
    indexes for HPAs and PodDisruptionBudgets once, then applies them to
    each Deployment in the namespace.

    Args:
        k8s: Kubernetes client instance.
        namespace: Namespace to search in.

    Yields:
        WorkloadState objects representing each discovered Deployment.
    """
    deployments: list[V1Deployment] = k8s.apps.list_namespaced_deployment(
        namespace=namespace
    ).items

    for deployment in deployments:
        yield WorkloadState(
            spec=get_workload_spec(deployment=deployment, is_full=False),
            runtime=get_workload_runtime(deployment, is_full=False),
        )


def discover_namespaces(config: DiscoveryNamespaceConfigSchema) -> List[NamespaceState]:
    """
    Discover given namespaces in the cluster and their workloads.

    This function performs a full traversal of the cluster:
    namespaces → workloads.

    Args:
        config: Agent discovery config schema.

    Yields:
        NamespaceState objects containing workloads for each namespace.
    """
    k8s = KubernetesClient()
    namespace_states: List[NamespaceState] = []

    for ns_name in config.namespaces:
        ns_state = NamespaceState(name=ns_name)

        for workload in discover_workloads(k8s, ns_name):
            ns_state.workloads.append(workload)

        namespace_states.append(ns_state)

    return namespace_states
