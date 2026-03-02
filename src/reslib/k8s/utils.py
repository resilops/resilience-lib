import logging
import math
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from kubernetes.client import V1Deployment, V1Node, V1Pod, V2HorizontalPodAutoscaler
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
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
    K8DeploymentKind,
)
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import (
    ContainerCrashedError,
    HpaNotConfiguredError,
    HpaScaledError,
    ReachedDesiredReplicaError,
    ReplicasRestoredError,
    WorkloadNotFound,
)
from reslib.k8s.schema import (
    ContainerSpec,
    HPAConfig,
    HPAMetricSpec,
    K8Condition,
    PDBConfig,
    ResourceRequirements,
    WorkloadPolicies,
    WorkloadSpec,
    WorkloadState,
    WorkloadStatus,
)
from reslib.k8s.snapshot import NamespaceSnapshot

logger = logging.getLogger(__name__)


def get_deployment_conditions(deployment: V1Deployment) -> List[Optional[K8Condition]]:
    """
    Convert Kubernetes Deployment status conditions into a JSON-friendly model.

    Kubernetes exposes rollout/availability information on a Deployment via
    `deployment.status.conditions`, where each condition is a Kubernetes client
    object (e.g. V1DeploymentCondition). This helper extracts the relevant
    fields and returns them as a list of `K8Condition` models that are safe to
    store, serialize, and consume in APIs/UI/AI analysis.

    Args:
        deployment: Kubernetes `V1Deployment` object.

    Returns:
        List of `K8Condition` objects. If the Deployment has no conditions,
        returns an empty list.
    """
    conditions: List[Optional[K8Condition]] = []
    for c in deployment.status.conditions or []:
        conditions.append(
            K8Condition(
                type=c.type,
                status=c.status,
                reason=getattr(c, "reason", None),
                message=getattr(c, "message", None),
                last_transition_time=getattr(c, "last_transition_time", None),
            )
        )
    return conditions


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
def get_namespace_snapshot(k8s: KubernetesClient, namespace: str) -> NamespaceSnapshot:
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
        k8s: KubernetesClient object.
        namespace: The Kubernetes namespace to take a snapshot.

    Returns:
        NamespaceSnapshot: Immutable object containing the HPAs and PDBs
        for the namespace.
    """
    hpas = k8s.autoscaling.list_namespaced_horizontal_pod_autoscaler(
        namespace=namespace
    ).items
    pdbs = k8s.policy.list_namespaced_pod_disruption_budget(namespace=namespace).items

    return NamespaceSnapshot(
        hpas={hpa.spec.scale_target_ref.name: hpa for hpa in hpas}, pdbs=pdbs
    )


def get_workload_spec(
    snapshot: NamespaceSnapshot, deployment: V1Deployment
) -> WorkloadSpec:
    """
    Build a WorkloadSpec from a Deployment, HPA index and get container resource/limits.

    Used during bulk discovery.
    """
    hpa = snapshot.get_hpa(deployment)
    containers: list[ContainerSpec] = []
    pod_spec = deployment.spec.template.spec

    for container in pod_spec.containers or []:
        res = getattr(container, "resources", None)
        requests = getattr(res, "requests", None)
        limits = getattr(res, "limits", None)
        containers.append(
            ContainerSpec(
                name=container.name,
                resources=(
                    ResourceRequirements(requests=requests, limits=limits)
                    if res
                    else None
                ),
            )
        )

    return WorkloadSpec(
        name=deployment.metadata.name,
        kind=K8DeploymentKind.DEPLOYMENT,
        replicas=deployment.spec.replicas or 0,
        hpa=(
            HPAConfig(
                name=hpa.metadata.name,
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
        labels=deployment.spec.selector.match_labels or {},
        containers=containers,
    )


def get_workload_policies(
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


def get_workload_status(deployment: V1Deployment) -> WorkloadStatus:
    """Build the current WorkloadStatus from a Deployment."""
    return WorkloadStatus(
        ready_replicas=deployment.status.ready_replicas or 0,
        is_available=is_deployment_available(deployment),
        reconciling=is_deployment_in_progress(deployment),
        is_faulty=is_deployment_faulty(deployment),
        spec_generation=deployment.metadata.generation,
        spec_applied_generation=deployment.status.observed_generation,
        conditions=get_deployment_conditions(deployment),
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
    snapshot = get_namespace_snapshot(k8s=k8s, namespace=namespace)

    try:
        deployment = k8s.apps.read_namespaced_deployment(
            name=name,
            namespace=namespace,
        )
    except ApiException as exc:
        if exc.status == 404:
            raise WorkloadNotFound(
                "Deployment not found",
                context={"deployment": name, "namespace": namespace},
            )
        raise

    return WorkloadState(
        spec=get_workload_spec(snapshot=snapshot, deployment=deployment),
        policies=get_workload_policies(snapshot=snapshot, deployment=deployment),
        status=get_workload_status(deployment),
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
    max_timeout: int = 300,
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
        raise ValueError("No pods given to calculate timeout")

    max_grace = max(
        pod.spec.termination_grace_period_seconds or default_grace_period
        for pod in pods
    )

    return min(max_grace + buffer_seconds, max_timeout)


def get_hpa_resource_metric(
    hpa: HPAConfig,
    metric_source: HpaMetricSourceEnum,
    resource_type: HpaResourceTypeEnum,
) -> Optional[HPAMetricSpec]:
    """
    Return the HPA metric matching the given metric type and resource.

    Args:
        hpa: HPA configuration containing metric specifications.
        metric_source: HPA metric type (e.g. RESOURCE).
        resource_type: Resource name (CPU or MEMORY).

    Returns:
        The matching HPAMetricSpec if found, otherwise None.
    """
    for metric in hpa.metrics:
        if (
            metric.type == metric_source
            and metric.resource.get("name") == resource_type.value
        ):
            return metric
    return None


def get_workload_pods(
    *,
    k8s: KubernetesClient,
    namespace: str,
    workload_spec: WorkloadSpec,
    pod_phase: Optional[str] = POD_RUNNING_STATUS,
) -> List[V1Pod]:
    """
    Return pods belonging to a workload filtered by pod phase.

    Kwargs:
        k8s: Kubernetes client wrapper.
        namespace: Namespace where the workload is deployed.
        label_selector: Label selector to get pod.
        pod_phase: Pod phase to filter by (default: Running).

    Returns:
        List of pods matching the workload selector and pod phase.
    """
    selector = ",".join(f"{key}={value}" for key, value in workload_spec.labels.items())
    pods = k8s.v1_api.list_namespaced_pod(namespace=namespace, label_selector=selector)

    if pod_phase is None:
        return [pod for pod in pods.items]

    return [pod for pod in pods.items if pod.status.phase == pod_phase]


def calculate_hpa_trigger(
    status: WorkloadStatus,
    metric: HPAMetricSpec,
    idle_cpu_pct: int,
    pod_cpu_stress_threshold_pct: Optional[int] = 95,
) -> Tuple[int, int]:
    """
    Calculate the minimal number of pods and CPU percentage per pod to trigger HPA.

    Args:
        status: Current workload status with ready replicas.
        metric: HPA CPU metric specification.
        idle_cpu_pct: Current CPU usage when idle (baseline).
        pod_cpu_stress_threshold_pct: Maximum allowable CPU load per pod (default 95%).

    Returns:
        Tuple[pods_to_stress, stress_cpu_percent]

    Raises:
        HpaNotConfiguredError: If HPA metric lacks 'averageUtilization'.
    """
    target: dict = metric.resource.get("target", {})
    average_utilization = target.get("average_utilization")

    if average_utilization is None:
        raise HpaNotConfiguredError(
            "Missing 'averageUtilization' or HPA is misconfigured."
        )

    replicas = status.ready_replicas
    total_required_cpu_increase = replicas * (average_utilization - idle_cpu_pct)

    # Start with stressing 1 pod
    for pods_to_stress in range(1, replicas + 1):
        stress_percent = idle_cpu_pct + total_required_cpu_increase / pods_to_stress
        stress_percent = math.ceil(stress_percent)
        if stress_percent <= pod_cpu_stress_threshold_pct:
            return pods_to_stress, stress_percent

    # If even all pods at max stress cannot reach target
    return replicas, pod_cpu_stress_threshold_pct


def get_hpa_current_average_utilization(
    hpa: V2HorizontalPodAutoscaler,
) -> Optional[int]:
    """
    Extract the average CPU utilization from HPA status metrics (autoscaling/v2).
    Returns None if not found or not a CPU metric.
    """
    for metric in hpa.status.current_metrics or []:
        if metric.type == HpaMetricSourceEnum.RESOURCE.value and metric.resource:
            return metric.resource.current.average_utilization
    return None


def raise_on_container_fail(
    k8s: KubernetesClient, workload_spec: WorkloadSpec, namespace: str
) -> None:
    """
    Check all pods in a deployment for container failures.

    Args:
        k8s: Kubernetes client instance used to query pods.
        workload_spec: The Kubernetes workload_spec containing deployment labels.
        namespace: Namespace where the deployment resides.

    Raises:
        ContainerCrashedError: If any container is in a crash, waiting with
        abnormal reason, or terminated with an unexpected reason or non-zero exit code.
    """
    pods: List[V1Pod] = get_workload_pods(
        k8s=k8s, workload_spec=workload_spec, namespace=namespace, pod_phase=None
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
                    "Pod container is waiting unexpectedly",
                    context={
                        "container": container_name,
                        "pod": pod_name,
                        "reason": state.waiting.reason,
                    },
                )

            # Check for terminated state with abnormal reasons
            if (
                state.terminated
                and state.terminated.reason not in POD_TERMINATED_REASONS_OK
            ):
                raise ContainerCrashedError(
                    "Pod terminated unexpectedly",
                    context={
                        "container": container_name,
                        "pod": pod,
                        "reason": state.terminated.reason,
                    },
                )


def raise_on_hpa_scale(
    k8s: KubernetesClient, namespace: str, workload: WorkloadState
) -> Optional[Dict[str, int]]:
    """
    Check if a deployment's ready replicas have increased above the starting count.

    Args:
        k8s: Kubernetes client instance.
        workload: Workload state.
        namespace: Kubernetes namespace of the deployment.

    Returns:
        Dict with 'before' state and 'after' state keys if replicas increased,
        otherwise None.

    Raises:
        HpaScaledError: If the number of ready replicas exceeds start_replicas.
    """
    deployment = k8s.apps.read_namespaced_deployment(
        name=workload.spec.name, namespace=namespace
    )
    start_replicas = workload.status.ready_replicas
    current_replicas = deployment.status.ready_replicas or 0

    if current_replicas > start_replicas:
        hpa = k8s.autoscaling.read_namespaced_horizontal_pod_autoscaler(
            name=workload.spec.hpa.name, namespace=namespace
        )
        raise HpaScaledError(
            "HPA scaled: replicas increased",
            context={
                "before_replicas": start_replicas,
                "after_replicas": current_replicas,
                "average_utilization": get_hpa_current_average_utilization(hpa=hpa),
            },
        )

    return None


def raise_on_desired_replicas(
    k8s: KubernetesClient,
    workload_name: str,
    namespace: str,
) -> None:
    """
    Raise an exception when a Deployment has reached (or exceeded) its desired
    number of replicas.

    This function is typically used as a guard or exit condition in polling /
    reconciliation loops that wait for a Kubernetes Deployment to become ready.

    Args:
        k8s: Initialized Kubernetes client.
        workload_name: Name of the Deployment to inspect.
        namespace: Kubernetes namespace containing the Deployment.

    Raises:
        ReachedDesiredReplicaError: If the number of ready replicas is greater
            than or equal to the desired replicas specified in the Deployment
            spec.
    """
    deployment = k8s.apps.read_namespaced_deployment(
        name=workload_name,
        namespace=namespace,
    )
    desired_replicas = deployment.spec.replicas or 0
    ready_replicas = deployment.status.ready_replicas or 0

    if ready_replicas >= desired_replicas:
        raise ReachedDesiredReplicaError(
            "Deployment has reached the desired number of replicas",
            context={
                "deployment": workload_name,
                "namespace": namespace,
                "ready_replicas": ready_replicas,
                "desired_replicas": desired_replicas,
            },
        )


def raise_on_replicas_restored_cpu(
    k8s: KubernetesClient,
    namespace: str,
    stress_context: Dict[Any, Any],
) -> None:
    """
    Raise when a Deployment's replicas have stabilized after HPA scaling.

    Assumes guardrails have already verified:
      - HPA exists
      - Deployment is healthy and available

    Args:
        k8s: Kubernetes client instance.
        namespace: Namespace of the workload.
        stress_context: Stress context, e.g., workload state, CPU usage during stress

    Raises:
        ReplicasRestoredError: If the Deployment replicas are considered restored.
    """
    initial_workload_state: WorkloadState = stress_context.get("workload")
    stress_average_utilization = stress_context.get("average_utilization")
    max_replicas_on_stress = stress_context.get("after_replicas")

    deployment = k8s.apps.read_namespaced_deployment(
        name=initial_workload_state.spec.name, namespace=namespace
    )
    hpa = k8s.autoscaling.read_namespaced_horizontal_pod_autoscaler(
        name=initial_workload_state.spec.hpa.name, namespace=namespace
    )

    current_replicas = deployment.status.ready_replicas or 0
    desired_replicas = hpa.status.desired_replicas or 0
    current_average_utilization = get_hpa_current_average_utilization(hpa)

    logger.info(f"Current CPU utilization: {current_average_utilization}")

    if (
        desired_replicas < max_replicas_on_stress
        and current_replicas < max_replicas_on_stress
        and current_average_utilization
        and current_average_utilization < stress_average_utilization
    ):
        raise ReplicasRestoredError(
            "Workload replicas have stabilized after HPA stress test.",
            context={
                "namespace": namespace,
                "deployment": initial_workload_state.spec.name,
                "current_replicas": current_replicas,
                "desired_replicas": desired_replicas,
                "stress_average_utilization": stress_average_utilization,
                "current_average_utilization": current_average_utilization,
            },
        )


def is_node_tolerated(node: V1Node, deployment: V1Deployment) -> bool:
    """
    Check if a pod with the deployment's tolerations can be scheduled on the node.

    Returns True if all node taints are tolerated.
    """
    if not node.spec.taints:
        return True  # No taints to worry about

    for taint in node.spec.taints:
        # If no toleration matches this taint, node is not tolerated
        tolerated = any(
            (not tol.effect or tol.effect == taint.effect)
            and (
                (tol.operator == "Exists" and (tol.key == taint.key or tol.key is None))
                or (
                    tol.operator == "Equal"
                    and tol.key == taint.key
                    and tol.value == taint.value
                )
            )
            for tol in deployment.spec.template.spec.tolerations or []
        )
        if not tolerated:
            return False  # Early exit: found an un-tolerated taint

    return True  # All taints tolerated


def get_schedulable_nodes(
    k8s: KubernetesClient, deployment: V1Deployment
) -> List[V1Node]:
    """
    Return only nodes that where pod can be scheduled and match the given toleration.

    Args:
        k8s: Kubernetes client instance.
        deployment: Deployment

    Returns:
        List of schedulable nodes.
    """
    schedulable_nodes: List[V1Node] = []
    for node in k8s.v1_api.list_node().items:
        if node.spec.unschedulable:
            continue
        if is_node_tolerated(node, deployment):
            schedulable_nodes.append(node)

    return schedulable_nodes
