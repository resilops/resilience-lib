import math
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from kubernetes import config as k8config
from kubernetes.client import V1Deployment, V1Pod
from kubernetes.client.exceptions import ApiException

from reslib.config import config
from reslib.constants import (
    DEPLOYMENT_CONDITION_AVAILABLE,
    DEPLOYMENT_CONDITION_PROGRESSING,
    DEPLOYMENT_STATUS_MIN_RS_AVAILABLE,
    DEPLOYMENT_STATUS_PROGRESS_DEADLINE,
    DEPLOYMENT_STATUS_RS_AVAILABLE,
    POD_RUNNING_STATUS,
    POD_TERMINATED_REASONS_OK,
    POD_WAITING_REASONS_OK,
    HpaMetricTypeEnum,
    HpaResourceNameEnum,
    K8DeploymentKind,
)
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import (
    ContainerCrashedError,
    HpaNotConfiguredError,
    HpaScaledError,
    WorkloadNotFound,
)
from reslib.k8s.schema import (
    HPAConfig,
    HPAMetricSpec,
    PDBConfig,
    WorkloadPolicies,
    WorkloadSpec,
    WorkloadState,
    WorkloadStatus,
)
from reslib.k8s.snapshot import NamespaceSnapshot


def current_cluster_name() -> str:
    """
    Get current active cluster name from the config

    Returns:
        Name of the cluster (string)
    """
    _, active_context = k8config.list_kube_config_contexts()
    return active_context.get("context", {}).get("cluster")


def is_deployment_in_progress(deployment: V1Deployment) -> bool:
    """
    Determine if a Deployment is currently rolling out.

    A deployment is considered "in progress" if:
      - Progressing=True
      - AND the reason is not "NewReplicaSetAvailable"

    Args:
        deployment: V1Deployment object.

    Returns:
        True if rollout is in progress, False otherwise.
    """
    conditions: List = deployment.status.conditions or []

    for cond in conditions:
        if (
            cond.type == DEPLOYMENT_CONDITION_PROGRESSING
            and cond.status == "True"
            and cond.reason != DEPLOYMENT_STATUS_RS_AVAILABLE
        ):
            return True

    return False


def is_deployment_available(deployment: V1Deployment) -> bool:
    """
    Determine if a Deployment is currently available.

    Args:
        deployment: V1Deployment object.

    Returns:
        True if serving traffic, False otherwise.
    """
    conditions: List = deployment.status.conditions or []
    for cond in conditions:
        if (
            cond.type == DEPLOYMENT_CONDITION_AVAILABLE
            and cond.status == "True"
            and cond.reason == DEPLOYMENT_STATUS_MIN_RS_AVAILABLE
        ):
            return True
    return False


def is_deployment_faulty(deployment: V1Deployment) -> bool:
    """
    Determine whether a Deployment is in a failed state.

    A Deployment is considered faulty if Kubernetes reports that
    the rollout has failed due to exceeding the progress deadline.

    Args:
        deployment: V1Deployment object.

    Returns:
        True if the Deployment is faulty, False otherwise.
    """
    conditions: List = deployment.status.conditions or []

    for cond in conditions:
        if (
            cond.type == DEPLOYMENT_CONDITION_PROGRESSING
            and cond.status == "False"
            and cond.reason == DEPLOYMENT_STATUS_PROGRESS_DEADLINE
        ):
            return True

    return False


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
                metrics=[
                    HPAMetricSpec(type=m.type, resource=m.resource.to_dict())
                    for m in (hpa.spec.metrics or [])
                ],
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
        ready_replicas=deployment.status.ready_replicas or 0,
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
    *,
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


def get_hpa_resource_metric(
    hpa: HPAConfig,
    metric_type: HpaMetricTypeEnum,
    resource: HpaResourceNameEnum,
) -> Optional[HPAMetricSpec]:
    """
    Return the HPA metric matching the given metric type and resource.

    Args:
        hpa: HPA configuration containing metric specifications.
        metric_type: HPA metric type (e.g. RESOURCE).
        resource: Resource name (CPU or MEMORY).

    Returns:
        The matching HPAMetricSpec if found, otherwise None.
    """
    for metric in hpa.metrics:
        if metric.type == metric_type and metric.resource.get("name") == resource.value:
            return metric
    return None


def get_deployment_pods(
    *,
    k8s: KubernetesClient,
    namespace: str,
    deployment: V1Deployment,
    pod_phase: Optional[str] = POD_RUNNING_STATUS,
) -> List[V1Pod]:
    """
    Return pods belonging to a workload filtered by pod phase.

    Kwargs:
        k8s: Kubernetes client wrapper.
        namespace: Namespace where the workload is deployed.
        deployment: Kubernetes Deployment object.
        pod_phase: Pod phase to filter by (default: Running).

    Returns:
        List of pods matching the workload selector and pod phase.
    """
    selector = ",".join(
        f"{key}={value}" for key, value in deployment.spec.selector.match_labels.items()
    )

    pods = k8s.v1_api.list_namespaced_pod(
        namespace=namespace,
        label_selector=selector,
    )

    if pod_phase is None:
        return [pod for pod in pods.items]

    return [pod for pod in pods.items if pod.status.phase == pod_phase]


def calculate_hpa_trigger(
    workload: WorkloadState,
    metric: HPAMetricSpec,
    idle_cpu_pct: int,
    max_cpu_stress_pct_per_pod: Optional[int] = 95,
) -> Tuple[int, int]:
    """
    Calculate the minimal number of pods and CPU percentage per pod to trigger HPA.

    Args:
        workload: Current workload state with ready replicas.
        metric: HPA CPU metric specification.
        idle_cpu_pct: Current CPU usage when idle (baseline).
        max_cpu_stress_pct_per_pod: Maximum allowable CPU load per pod (default 95%).

    Returns:
        Tuple[pods_to_stress, stress_cpu_percent]

    Raises:
        HpaNotConfiguredError: If HPA metric lacks 'averageUtilization'.
    """
    target: dict = metric.resource.get("target", {})
    average_utilization = target.get("average_utilization")

    if average_utilization is None:
        raise HpaNotConfiguredError(
            "Missing 'averageUtilization'. HPA is misconfigured."
        )

    replicas = workload.status.ready_replicas
    total_required_cpu_increase = replicas * (average_utilization - idle_cpu_pct)

    # Start with stressing 1 pod
    for pods_to_stress in range(1, replicas + 1):
        stress_percent = idle_cpu_pct + total_required_cpu_increase / pods_to_stress
        stress_percent = math.ceil(stress_percent)
        if stress_percent <= max_cpu_stress_pct_per_pod:
            return pods_to_stress, stress_percent

    # If even all pods at max stress cannot reach target
    return replicas, max_cpu_stress_pct_per_pod


def raise_on_container_fail(
    k8s: KubernetesClient, deployment: V1Deployment, namespace: str
) -> None:
    """
    Check all pods in a deployment for container failures.

    Args:
        k8s: Kubernetes client instance used to query pods.
        deployment: The Kubernetes deployment object to inspect.
        namespace: Namespace where the deployment resides.

    Raises:
        ContainerCrashedError: If any container is in a crash, waiting with
        abnormal reason, or terminated with an unexpected reason or non-zero exit code.
    """
    pods: List[V1Pod] = get_deployment_pods(
        k8s=k8s, deployment=deployment, namespace=namespace, pod_phase=None
    )

    for pod in pods:
        pod_name = pod.metadata.name
        container_statuses = pod.status.container_statuses or []
        for cs in container_statuses:
            container_name = cs.name
            state = cs.state
            # Check for waiting state with abnormal reasons
            if state.waiting and state.waiting.reason not in POD_WAITING_REASONS_OK:
                raise ContainerCrashedError(
                    f"Pod '{pod_name}' container "
                    f"'{container_name}' is waiting "
                    f"unexpectedly: {state.waiting.reason}"
                )

            # Check for terminated state with abnormal reasons
            if (
                state.terminated
                and state.terminated.reason not in POD_TERMINATED_REASONS_OK
            ):
                raise ContainerCrashedError(
                    f"Pod '{pod_name}' container '{container_name}' "
                    f"terminated unexpectedly (reason={state.terminated.reason}, "
                    f"exit_code={state.terminated.exit_code})"
                )


def raise_on_hpa_scale(
    k8s: KubernetesClient, workload_name: str, namespace: str, start_replicas: int
) -> Optional[Dict[str, int]]:
    """
    Check if a deployment's ready replicas have increased above the starting count.

    Args:
        k8s: Kubernetes client instance.
        workload_name: Name of the deployment/workload.
        namespace: Kubernetes namespace of the deployment.
        start_replicas: The initial number of ready replicas.

    Returns:
        Dict with 'before' and 'after' keys if replicas increased, otherwise None.

    Raises:
        HpaScaledError: If the number of ready replicas exceeds start_replicas.
    """
    deployment = k8s.apps.read_namespaced_deployment(
        name=workload_name, namespace=namespace
    )
    current_replicas = deployment.status.ready_replicas or 0

    if current_replicas > start_replicas:
        raise HpaScaledError(
            "HPA scaled: replicas increased",
            before=start_replicas,
            after=current_replicas,
        )

    return None
