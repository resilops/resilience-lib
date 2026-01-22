from typing import Dict, Generator, Optional
from kubernetes.client import (
    V1Deployment, V2HorizontalPodAutoscaler, V1PodDisruptionBudget
)
from reslib.constants import K8DeploymentKind
from reslib.k8s.client import KubernetesClient
from reslib.k8s.status import (
    current_cluster_name,
    ready_replicas,
    is_deployment_condition_true
)
from reslib.k8s.schema import (
    ClusterState,
    NamespaceState,
    WorkloadState,
    WorkloadSpec,
    WorkloadPolicies,
    WorkloadStatus,
    HPAConfig,
    PDBConfig,
)

__all__ = ["discover_namespaces", "discover_workloads", "discover_cluster"]


def _get_namespace_hpa_indexes(
    k8s: KubernetesClient,
    namespace: str,
    kind: K8DeploymentKind = K8DeploymentKind.STATELESS
) -> Dict[str, V2HorizontalPodAutoscaler]:
    """
    Build a mapping of deployment name -> HPA object for a namespace.

    Args:
        k8s: Kubernetes client instance.
        namespace: Namespace name.
        kind: deployment kind

    Returns:
        Dict mapping deployment names to their HPA objects.
    """
    hpas = k8s.autoscaling.list_namespaced_horizontal_pod_autoscaler(
        namespace=namespace
    ).items
    return {
        hpa.spec.scale_target_ref.name: hpa
        for hpa in hpas
        if hpa.spec.scale_target_ref.kind == kind.value
    }


def _get_namespace_pdb_indexes(
    k8s: KubernetesClient, namespace: str
) -> Dict[str, V1PodDisruptionBudget]:
    """
    Build a mapping of deployment name -> PodDisruptionBudget object for a namespace.

    Args:
        k8s: Kubernetes client instance.
        namespace: Namespace name.

    Returns:
        Dict mapping deployment names to their PDB objects.
    """
    pdbs = k8s.policy.list_namespaced_pod_disruption_budget(
        namespace=namespace
    ).items
    return {
        pdb.spec.selector.match_labels.get("app", pdb.metadata.name): pdb
        for pdb in pdbs
    }


def _build_workload_spec(
    deployment: V1Deployment, hpa_index: Dict[str, V2HorizontalPodAutoscaler]
) -> WorkloadSpec:
    """
    Build an internal WorkloadSpec object for a Kubernetes deployment.

    This includes basic spec info and optional HPA configuration.

    Args:
        deployment: Kubernetes V1Deployment object.
        hpa_index: Mapping from deployment name to V2HorizontalPodAutoscaler.

    Returns:
        WorkloadSpec instance representing the deployment spec.
    """
    dep_name = deployment.metadata.name
    hpa = hpa_index.get(dep_name)

    return WorkloadSpec(
        name=dep_name,
        kind=K8DeploymentKind.STATELESS.value,  # Can be extended to detect stateful
        replicas=deployment.spec.replicas or 0,
        hpa=HPAConfig(
            min_replicas=hpa.spec.min_replicas,
            max_replicas=hpa.spec.max_replicas
        ) if hpa else None,
    )


def _build_workload_policies(
    deployment: V1Deployment, pdb_index: Dict[str, V1PodDisruptionBudget]
) -> WorkloadPolicies:
    """
    Build internal WorkloadPolicies for a deployment, including PDB if present.

    Args:
        deployment: Kubernetes V1Deployment object.
        pdb_index: Mapping from deployment name to V1PodDisruptionBudget.

    Returns:
        WorkloadPolicies instance.
    """
    dep_name = deployment.metadata.name
    pdb = pdb_index.get(dep_name)

    return WorkloadPolicies(
        pdb=PDBConfig(
            min_available=pdb.spec.min_available,
            max_unavailable=pdb.spec.max_unavailable
        ) if pdb else None
    )


def _build_workload_status(deployment: V1Deployment) -> WorkloadStatus:
    """
    Build the current status of a deployment.

    Args:
        deployment: Kubernetes V1Deployment object.

    Returns:
        WorkloadStatus instance with ready, serving, and reconciling info.
    """
    return WorkloadStatus(
        ready_replicas=ready_replicas(deployment),
        serving_traffic=is_deployment_condition_true(
            deployment, condition_type="Available"
        ),
        reconciling=is_deployment_condition_true(
            deployment, condition_type="Progressing"
        ),
        spec_generation=deployment.metadata.generation,
        spec_applied_generation=deployment.status.observed_generation,
    )


def discover_workloads(
    k8s_client: KubernetesClient,
    namespace: str,
    labels: Optional[str] = None
) -> Generator[WorkloadState, None, None]:
    """
    Yield all workloads in a namespace with spec, policies, and current status.

    Args:
        k8s_client: Kubernetes client instance.
        namespace: Namespace to list workloads from.
        labels: Optional label selector to filter workloads.
            Example: "app=myapp,tier=backend"

    Yields:
        WorkloadState objects for each deployment in the namespace.
    """
    hpa_index = _get_namespace_hpa_indexes(k8s=k8s_client, namespace=namespace)
    pdb_index = _get_namespace_pdb_indexes(k8s=k8s_client, namespace=namespace)

    params = {"namespace": namespace}
    if labels:
        params["label_selector"] = labels

    deployments = k8s_client.apps.list_namespaced_deployment(**params).items

    for dep in deployments:
        yield WorkloadState(
            spec=_build_workload_spec(dep, hpa_index),
            policies=_build_workload_policies(dep, pdb_index),
            status=_build_workload_status(dep),
        )


def discover_namespaces(
    k8s_client: KubernetesClient
) -> Generator[NamespaceState, None, None]:
    """
    Yield all namespaces in the cluster, each with its workloads.

    Args:
        k8s_client: Kubernetes client instance.

    Yields:
        NamespaceState objects with workload mapping.
    """
    namespaces = k8s_client.v1_api.list_namespace().items
    for ns in namespaces:
        ns_state = NamespaceState(
            name=ns.metadata.name,
            labels=ns.metadata.labels or {}
        )
        for workload in discover_workloads(k8s_client, ns.metadata.name):
            ns_state.workloads[workload.spec.name] = workload
        yield ns_state


def discover_cluster(k8s_client: KubernetesClient) -> ClusterState:
    """
    Build a complete cluster state with namespaces and workloads.

    Args:
        k8s_client: Kubernetes client instance.

    Returns:
        ClusterState object representing the current cluster.
    """
    cluster_state = ClusterState(name=current_cluster_name())
    for ns_state in discover_namespaces(k8s_client):
        cluster_state.namespaces[ns_state.name] = ns_state
    return cluster_state
