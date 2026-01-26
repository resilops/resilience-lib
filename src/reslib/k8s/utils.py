from typing import List, Optional

from kubernetes.client import V1Pod
from kubernetes.client.exceptions import ApiException

from reslib.config import config
from reslib.k8s.client import KubernetesClient
from reslib.k8s.discovery import discover_workloads
from reslib.k8s.exceptions import MultipleWorkloadsReturned, WorkloadNotFound
from reslib.k8s.schema import WorkloadState


def get_single_workload(
    *, namespace: str, labels: str, k8s: Optional[KubernetesClient] = None
) -> WorkloadState:
    """
    Discover and return exactly one Kubernetes workload matching the given labels.

    Args:
        namespace: Kubernetes namespace to search in.
        labels: Label selector used to identify the workload.
        k8s: Optional KubernetesClient instance. If not provided, a new one is created.

    Returns:
        The single matching WorkloadState.

    Raises:
        WorkloadNotFound: If no workloads match the label selector.
        MultipleWorkloadsReturned: If more than one workload matches the selector.
    """
    workloads: List[WorkloadState] = list(
        discover_workloads(
            k8s or KubernetesClient(),
            namespace=namespace,
            labels=labels,
        )
    )

    if not workloads:
        raise WorkloadNotFound(f"No workloads found for labels '{labels}'")

    if len(workloads) > 1:
        raise MultipleWorkloadsReturned(
            f"Multiple workloads returned for labels '{labels}'"
        )

    return workloads[0]


def pod_exists(
    namespace: str, pod_name: str, k8s: Optional[KubernetesClient] = None
) -> True:
    """
    Fetch a pod by name from a given namespace. Returns None if the pod does not exist.

    Args:
        namespace: Kubernetes namespace of the pod.
        pod_name: Name of the pod.
        k8s: Optional KubernetesClient instance. If not provided, a new one is created.

    Returns:
        V1Pod object if found, otherwise None.

    Raises:
        ApiException: Propagates API exceptions other than 404 (NotFound).
    """
    k8s = k8s or KubernetesClient()
    try:
        k8s.v1_api.read_namespaced_pod(name=pod_name, namespace=namespace)
        return True
    except ApiException as e:
        if e.status == 404:
            return False
        raise


def get_pod_termination_timeout(
    pods: List[V1Pod],
    buffer_seconds: int = 10,
    default_grace_period: int = config.pod_termination_default_grace_period,
    max_timeout: int = config.pod_termination_max_timeout,
) -> int:
    """
    Calculate the effective timeout to wait for pod termination.

    The timeout is derived from the maximum `terminationGracePeriodSeconds`
    among the given pods, plus a safety buffer, and capped at `max_timeout`.

    Args:
        pods: List of Pod objects to be terminated.
        buffer_seconds: Extra seconds added on top of the grace period.
        default_grace_period: Fallback grace period if pod spec does not define one.
        max_timeout: Upper bound for the returned timeout.

    Returns:
        Timeout in seconds to wait for pod termination.
    """
    max_grace = max(
        (pod.spec.termination_grace_period_seconds or default_grace_period)
        for pod in pods
    )
    return min(max_grace + buffer_seconds, max_timeout)
