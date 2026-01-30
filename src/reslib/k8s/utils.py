from functools import lru_cache
from typing import List, Optional

from kubernetes.client import V1Deployment, V1Pod
from kubernetes.client.exceptions import ApiException

from reslib.config import config
from reslib.constants import K8DeploymentKind
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import WorkloadNotFound
from reslib.k8s.schema import (
    HPAConfig,
    PDBConfig,
    WorkloadPolicies,
    WorkloadSpec,
    WorkloadState,
    WorkloadStatus,
)
from reslib.k8s.snapshot import NamespaceSnapshot
from reslib.k8s.status import (
    is_deployment_available,
    is_deployment_faulty,
    is_deployment_in_progress,
    ready_replicas,
)


@lru_cache(maxsize=64)
def get_namespace_snapshot(namespace: str) -> NamespaceSnapshot:
    """
    Build and cache a snapshot of namespace-scoped workload policies.

    This function fetches all HorizontalPodAutoscalers (HPAs) and
    PodDisruptionBudgets (PDBs) in the given namespace and returns
    a `NamespaceSnapshot` containing:

        - `hpas`: a mapping of deployment name -> HPA
        - `pdbs`: a list of all PDBs in the namespace

    The result is cached per namespace using an LRU cache to avoid
    repeated API calls. The cache can store up to 64 namespaces.

    Args:
        namespace: The Kubernetes namespace to snapshot.

    Returns:
        NamespaceSnapshot: Immutable object containing the HPAs and PDBs
        for the namespace.
    """
    k8s = KubernetesClient()
    hpas = k8s.autoscaling.list_namespaced_horizontal_pod_autoscaler(
        namespace=namespace
    ).items
    pdbs = k8s.policy.list_namespaced_pod_disruption_budget(namespace=namespace).items

    return NamespaceSnapshot(
        hpas={hpa.spec.scale_target_ref.name: hpa for hpa in hpas}, pdbs=pdbs
    )


def build_workload_spec(
    snapshot: NamespaceSnapshot, deployment: V1Deployment
) -> WorkloadSpec:
    """
    Build a WorkloadSpec from a Deployment and HPA index.

    Used during bulk discovery.
    """
    hpa = snapshot.get_hpa(deployment)

    return WorkloadSpec(
        name=deployment.metadata.name,
        kind=K8DeploymentKind.DEPLOYMENT.value,
        replicas=deployment.spec.replicas or 0,
        hpa=(
            HPAConfig(
                min_replicas=hpa.spec.min_replicas,
                max_replicas=hpa.spec.max_replicas,
            )
            if hpa
            else None
        ),
    )


def build_workload_policies(
    snapshot: NamespaceSnapshot, deployment: V1Deployment
) -> WorkloadPolicies:
    """
    Build WorkloadPolicies from a Deployment and PDB index.

    Used during bulk discovery.
    """
    pod_labels = deployment.spec.template.metadata.labels or {}
    pdb = snapshot.get_pdb(pod_labels)
    return WorkloadPolicies(
        pdb=(
            PDBConfig(
                min_available=pdb.spec.min_available,
                max_unavailable=pdb.spec.max_unavailable,
            )
            if pdb
            else None
        )
    )


def build_workload_status(deployment: V1Deployment) -> WorkloadStatus:
    """Build the current WorkloadStatus from a Deployment."""
    return WorkloadStatus(
        ready_replicas=ready_replicas(deployment),
        is_available=is_deployment_available(deployment),
        reconciling=is_deployment_in_progress(deployment),
        is_faulty=is_deployment_faulty(deployment),
        spec_generation=deployment.metadata.generation,
        spec_applied_generation=deployment.status.observed_generation,
    )


def get_workload(
    *,
    namespace: str,
    name: str,
    k8s: Optional[KubernetesClient] = None,
) -> WorkloadState:
    """
    Fetch exactly one workload by name.

    This function is optimized for single-workload access and avoids
    namespace-wide indexing.

    Args:
        namespace: Kubernetes namespace.
        name: Deployment name.
        k8s: Optional Kubernetes client instance.

    Returns:
        WorkloadState representing the Deployment.

    Raises:
        WorkloadNotFound: If the Deployment does not exist.
    """
    k8s = k8s or KubernetesClient()
    snapshot = get_namespace_snapshot(namespace=namespace)

    try:
        deployment = k8s.apps.read_namespaced_deployment(
            name=name,
            namespace=namespace,
        )
    except ApiException as exc:
        if exc.status == 404:
            raise WorkloadNotFound(
                f"Deployment '{name}' not found in namespace '{namespace}'"
            )
        raise

    return WorkloadState(
        spec=build_workload_spec(snapshot=snapshot, deployment=deployment),
        policies=build_workload_policies(snapshot=snapshot, deployment=deployment),
        status=build_workload_status(deployment),
    )


def pod_exists(
    namespace: str,
    pod_name: str,
    k8s: Optional[KubernetesClient] = None,
) -> bool:
    """
    Check whether a Pod exists in a namespace.

    Args:
        namespace: Namespace name.
        pod_name: Pod name.
        k8s: Optional Kubernetes client.

    Returns:
        True if the Pod exists, False otherwise.
    """
    k8s = k8s or KubernetesClient()

    try:
        k8s.v1_api.read_namespaced_pod(name=pod_name, namespace=namespace)
        return True
    except ApiException as exc:
        if exc.status == 404:
            return False
        raise


def get_pod_termination_timeout(
    pods: List[V1Pod],
    buffer_seconds: int = 10,
    default_grace_period: int = config.pod_termination_default_grace_period,
    max_timeout: int = config.pod_termination_max_timeout,
) -> int:
    """
    Compute a safe timeout for Pod termination.

    The timeout is calculated as:
        max(terminationGracePeriodSeconds) + buffer_seconds

    The result is capped at max_timeout.

    Args:
        pods: Pods being terminated.
        buffer_seconds: Extra safety buffer in seconds.
        default_grace_period: Fallback grace period.
        max_timeout: Upper bound for the timeout.

    Returns:
        Timeout in seconds.
    """
    if not pods:
        return min(default_grace_period + buffer_seconds, max_timeout)

    max_grace = max(
        pod.spec.termination_grace_period_seconds or default_grace_period
        for pod in pods
    )

    return min(max_grace + buffer_seconds, max_timeout)
