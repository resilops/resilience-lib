import asyncio
import logging
import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from cachetools import TTLCache
from cachetools.keys import hashkey
from kubernetes import watch
from kubernetes.client import (
    V1Deployment,
    V1Pod,
    V1Probe,
    V1Service,
    V2HorizontalPodAutoscaler,
)
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
    WorkloadStatusEnum,
)
from reslib.core.context import get_context, set_context
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import (
    ContainerCrashedError,
    HpaNotConfiguredError,
    HpaScalePodReadyError,
    ReachedDesiredReplicaError,
    ReplicasRestoredError,
    WorkloadNotFound,
)
from reslib.k8s.schema import (
    ContainerHealthSpec,
    ContainerSpec,
    HPAConfig,
    HPAMetricSpec,
    K8Condition,
    PDBConfig,
    ProbeHttpGet,
    ResourceRequirements,
    WorkloadPolicies,
    WorkloadRuntimeState,
    WorkloadSpec,
    WorkloadState,
)
from reslib.k8s.snapshot import NamespaceSnapshot

logger = logging.getLogger(__name__)

_namespace_cache = TTLCache(maxsize=64, ttl=30)  # seconds
_namespace_cache_lock = asyncio.Lock()


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


def get_latest_pod_ready_time(pods: List[V1Pod]) -> Optional[datetime]:
    """
    Return the most recent Ready condition transition time across the given pods.
    """
    ready_times: List[datetime] = []

    for pod in pods:
        for condition in pod.status.conditions or []:
            if (
                condition.type == "Ready"
                and condition.status == "True"
                and getattr(condition, "last_transition_time", None) is not None
            ):
                ready_times.append(condition.last_transition_time)

    if not ready_times:
        return None

    return max(ready_times)


async def get_pods_by_labels(
    *,
    k8s: KubernetesClient,
    namespace: str,
    labels: Optional[Dict[str, str]],
    pod_phase: Optional[str] = POD_RUNNING_STATUS,
) -> List[V1Pod]:
    """
    Return pods in a namespace matching the given labels and optional phase.
    """
    selector = ",".join(f"{key}={value}" for key, value in (labels or {}).items())
    pods = await k8s.list_namespaced_pod(namespace=namespace, label_selector=selector)

    if pod_phase is None:
        return [pod for pod in pods.items]

    return [pod for pod in pods.items if pod.status.phase == pod_phase]


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


async def get_namespace_policies_snapshot(
    k8s: KubernetesClient, namespace: str
) -> NamespaceSnapshot:
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
    cache_key = hashkey(k8s, namespace)
    cached_snapshot = _namespace_cache.get(cache_key)
    if cached_snapshot is not None:
        return cached_snapshot

    hpa_list, pdb_list = await asyncio.gather(
        k8s.list_namespaced_horizontal_pod_autoscaler(namespace=namespace),
        k8s.list_namespaced_pod_disruption_budget(namespace=namespace),
    )

    snapshot = NamespaceSnapshot(
        hpas={hpa.spec.scale_target_ref.name: hpa for hpa in hpa_list.items},
        pdbs=pdb_list.items,
    )

    async with _namespace_cache_lock:
        existing_snapshot = _namespace_cache.get(cache_key)
        if existing_snapshot is not None:
            return existing_snapshot
        _namespace_cache[cache_key] = snapshot

    return snapshot


def _probe_http_get(
    probe: Optional[V1Probe],
    service_name: Optional[str],
    namespace: Optional[str],
) -> Optional[ProbeHttpGet]:
    """Build the http probe endpoints"""
    if not probe or not probe.http_get:
        return None

    http_get = probe.http_get

    host = getattr(http_get, "host", None)
    if not host and service_name and namespace:
        host = f"{service_name}.{namespace}.svc.cluster.local"

    return ProbeHttpGet(
        path=getattr(http_get, "path", None) or "/",
        port=getattr(http_get, "port", None),
        host=host,
        scheme=(getattr(http_get, "scheme", None) or "HTTP").lower(),
    )


def _build_container_specs(
    deployment: V1Deployment, service_name: str, is_full: bool = True
) -> List[ContainerSpec]:
    """Build container specs from a deployment pod template."""
    containers: list[ContainerSpec] = []
    pod_spec = deployment.spec.template.spec
    namespace = deployment.metadata.namespace

    for container in pod_spec.containers or []:
        res = getattr(container, "resources", None)
        requests = getattr(res, "requests", None)
        limits = getattr(res, "limits", None)
        containers.append(
            ContainerSpec(
                name=container.name,
                resources=(
                    ResourceRequirements(requests=requests, limits=limits)
                    if res and is_full
                    else None
                ),
                health=ContainerHealthSpec(
                    readiness=_probe_http_get(
                        getattr(container, "readiness_probe", None),
                        service_name,
                        namespace,
                    ),
                    liveness=_probe_http_get(
                        getattr(container, "liveness_probe", None),
                        service_name,
                        namespace,
                    ),
                    startup=(
                        _probe_http_get(
                            getattr(container, "startup_probe", None),
                            service_name,
                            namespace,
                        )
                        if is_full
                        else None
                    ),
                ),
            )
        )
    return containers


def _build_hpa_config(
    deployment: V1Deployment,
    snapshot: Optional[NamespaceSnapshot],
) -> Optional[HPAConfig]:
    """Build HPA config for a deployment from the namespace snapshot."""
    hpa = snapshot.get_hpa(deployment) if snapshot else None
    if hpa is None:
        return None

    return HPAConfig(
        name=hpa.metadata.name,
        min_replicas=hpa.spec.min_replicas,
        max_replicas=hpa.spec.max_replicas,
        metrics=[
            HPAMetricSpec(type=m.type, resource=m.resource.to_dict())
            for m in (hpa.spec.metrics or [])
        ],
    )


def get_service_name(
    deployment: V1Deployment,
    services: Optional[List[V1Service]] = None,
) -> Optional[str]:
    """Return a deterministic service name matching the deployment pod labels."""
    if not services:
        return None

    pod_labels = deployment.spec.template.metadata.labels or {}
    matching_service_names = [
        service.metadata.name
        for service in services
        if service.spec.selector
        and all(
            pod_labels.get(key) == value for key, value in service.spec.selector.items()
        )
    ]

    return min(matching_service_names) if matching_service_names else None


def get_workload_spec(
    *,
    deployment: V1Deployment,
    snapshot: Optional[NamespaceSnapshot] = None,
    services: Optional[List[V1Service]] = None,
    is_full: bool = True,
) -> WorkloadSpec:
    """
    Build a workload specification from a Kubernetes Deployment.

    Args:
        deployment:
            Kubernetes Deployment object.

        snapshot:
            Optional namespace snapshot used to resolve HPA configuration.
            Only used in `full` mode.

        services:
            Services

        is_full:
            Level of detail to include in the returned workload spec.

    Returns:
        WorkloadSpec:
            Normalized workload specification derived from the Deployment.
    """
    service_name: str = get_service_name(deployment, services)
    return WorkloadSpec(
        name=deployment.metadata.name,
        service_name=service_name,
        kind=K8DeploymentKind.DEPLOYMENT,
        replicas=deployment.spec.replicas or 0,
        hpa=_build_hpa_config(deployment, snapshot) if is_full else None,
        labels=deployment.spec.selector.match_labels if is_full else None,
        containers=_build_container_specs(deployment, service_name, is_full=is_full),
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


def get_workload_runtime(
    deployment: V1Deployment, is_full: bool = True
) -> WorkloadRuntimeState:
    """Build the current WorkloadRuntimeState from a Deployment."""

    is_available = is_deployment_available(deployment)
    reconciling = is_deployment_in_progress(deployment)
    is_faulty = is_deployment_faulty(deployment)

    # Priority-based status resolution
    if is_faulty:
        status = WorkloadStatusEnum.degraded
    elif reconciling:
        status = WorkloadStatusEnum.reconciling
    elif is_available:
        status = WorkloadStatusEnum.healthy
    else:
        status = WorkloadStatusEnum.unavailable

    return WorkloadRuntimeState(
        ready_replicas=deployment.status.ready_replicas or 0,
        status=status,
        generation=deployment.metadata.generation,
        observed_generation=deployment.status.observed_generation,
        conditions=get_deployment_conditions(deployment) if is_full else None,
    )


async def get_workload(
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
    snapshot, service_list = await asyncio.gather(
        get_namespace_policies_snapshot(k8s=k8s, namespace=namespace),
        k8s.list_namespaced_service(namespace=namespace),
    )

    try:
        deployment = await k8s.read_namespaced_deployment(
            name=name,
            namespace=namespace,
        )
    except ApiException as exc:
        if exc.status == 404:
            raise WorkloadNotFound(
                error_code="WORKLOAD_NOT_FOUND",
                message="Requested workload deployment was not found.",
                namespace=namespace,
                workload=name,
                context={
                    "rule": "deployment exists in namespace",
                    "inputs": {
                        "deployment": name,
                        "namespace": namespace,
                    },
                    "observed": {
                        "deployment_found": False,
                    },
                },
                fix_hint=(
                    "Verify the deployment name and namespace, or ensure the workload "
                    "has been created before running this operation."
                ),
                retryable=False,
            )
        raise

    return WorkloadState(
        spec=get_workload_spec(
            snapshot=snapshot,
            services=service_list.items,
            deployment=deployment,
        ),
        policies=get_workload_policies(snapshot=snapshot, deployment=deployment),
        runtime=get_workload_runtime(deployment),
    )


async def pod_exists(
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
        await k8s.read_namespaced_pod(name=pod_name, namespace=namespace)
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


async def get_workload_pods(
    *,
    k8s: KubernetesClient,
    namespace: str,
    workload_spec: WorkloadSpec,
    pod_phase: Optional[str] = POD_RUNNING_STATUS,
) -> List[V1Pod]:
    """
    Return pods belonging to a workload filtered by pod phase.

    The Kubernetes Python client is synchronous, so the underlying API request
    is offloaded to a worker thread by `KubernetesClient`.
    """
    return await get_pods_by_labels(
        k8s=k8s,
        namespace=namespace,
        labels=workload_spec.labels,
        pod_phase=pod_phase,
    )


def calculate_hpa_trigger(
    status: WorkloadRuntimeState,
    metric: HPAMetricSpec,
    idle_cpu_pct: int,
    cpu_stress_threshold_pct: Optional[int] = 95,
) -> Tuple[int, int]:
    """
    Calculate the minimal number of pods and CPU percentage per pod to trigger HPA.

    Args:
        status: Current workload runtime state with ready replicas.
        metric: HPA CPU metric specification.
        idle_cpu_pct: Current CPU usage when idle (baseline).
        cpu_stress_threshold_pct: Maximum allowable CPU load per pod (default 95%).

    Returns:
        Tuple[pods_to_stress, stress_cpu_percent]

    Raises:
        HpaNotConfiguredError: If HPA metric lacks 'averageUtilization'.
    """
    scenario = get_context("scenario")
    target: dict = metric.resource.get("target", {})
    average_utilization = target.get("average_utilization")

    if average_utilization is None:
        raise HpaNotConfiguredError(
            error_code="HPA_AVERAGE_UTILIZATION_MISSING",
            message="HPA metric configuration is missing averageUtilization target.",
            namespace=scenario.template.namespace,
            workload=scenario.template.workload,
            context={
                "rule": "HPA metric.resource.target.averageUtilization must be defined",
                "inputs": {
                    "metric_type": getattr(metric, "type", "resource"),
                },
                "observed": {
                    "average_utilization": None,
                    "metric_target": metric.resource.get("target", {}),
                },
            },
            fix_hint=(
                "Configure the HPA resource metric with "
                "`target.averageUtilization` before running CPU stress tests."
            ),
            retryable=False,
        )

    replicas = status.ready_replicas
    total_required_cpu_increase = replicas * (average_utilization - idle_cpu_pct)

    # Start with stressing 1 pod
    for pods_to_stress in range(1, replicas + 1):
        stress_percent = idle_cpu_pct + total_required_cpu_increase / pods_to_stress
        stress_percent = math.ceil(stress_percent)
        if stress_percent <= cpu_stress_threshold_pct:
            return pods_to_stress, stress_percent

    # If even all pods at max stress cannot reach target
    return replicas, cpu_stress_threshold_pct


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


async def raise_on_container_fail(
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
    pods: List[V1Pod] = await get_workload_pods(
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
                    error_code="CONTAINER_WAITING_UNEXPECTED",
                    message="Container entered an unexpected waiting state.",
                    namespace=namespace,
                    workload=workload_spec.name,
                    context={
                        "rule": (
                            "container waiting reason must be in allowed waiting states"
                        ),
                        "inputs": {
                            "namespace": namespace,
                            "workload": workload_spec.name,
                        },
                        "observed": {
                            "pod": pod_name,
                            "container": container_name,
                            "waiting_reason": state.waiting.reason,
                        },
                    },
                    fix_hint=(
                        "Inspect pod events and container logs to determine why the "
                        "container is blocked (e.g., ImagePullBackOff, "
                        "CrashLoopBackOff)."
                    ),
                    retryable=False,
                )

            # Check for terminated state with abnormal reasons
            if (
                state.terminated
                and state.terminated.reason not in POD_TERMINATED_REASONS_OK
            ):
                raise ContainerCrashedError(
                    error_code="CONTAINER_TERMINATED_UNEXPECTED",
                    message="Container terminated with an unexpected exit condition.",
                    namespace=namespace,
                    workload=workload_spec.name,
                    context={
                        "rule": (
                            "container termination reason must be "
                            "allowed and exit_code == 0"
                        ),
                        "inputs": {
                            "namespace": namespace,
                            "workload": workload_spec.name,
                        },
                        "observed": {
                            "pod": pod_name,
                            "container": container_name,
                            "termination_reason": state.terminated.reason,
                            "exit_code": state.terminated.exit_code,
                        },
                    },
                    fix_hint=(
                        "Check container logs and recent deployment changes. "
                        "Restart or roll back the workload if crashes persist."
                    ),
                    retryable=False,
                )


def _extract_event_timestamp(event_obj: Any) -> Optional[datetime]:
    """Extract the best available timestamp from a Kubernetes Event object."""
    if getattr(event_obj, "event_time", None):
        return event_obj.event_time

    series = getattr(event_obj, "series", None)
    if series and getattr(series, "last_observed_time", None):
        return series.last_observed_time

    if getattr(event_obj, "last_timestamp", None):
        return event_obj.last_timestamp

    if getattr(event_obj, "first_timestamp", None):
        return event_obj.first_timestamp

    return None


async def set_hpa_scale_event_context(
    k8s: KubernetesClient,
    namespace: str,
    workload: WorkloadState,
    not_before: Optional[datetime] = None,
) -> Optional[Dict]:
    """
    Watch Kubernetes Events for HPA scale-up (SuccessfulRescale), then
    store the event timestamp in context.

    Args:
        k8s: Kubernetes client instance.
        namespace: Namespace of the HPA/workload.
        workload: Workload state.
        not_before: Not before timestamp.

    Returns:
        Dict with event metadata if a matching event is found, otherwise None.
    """
    hpa_name = workload.spec.hpa.name
    api = k8s.new_v1_api()
    w = watch.Watch()

    field_selector = (
        f"involvedObject.kind=HorizontalPodAutoscaler,"
        f"involvedObject.name={hpa_name}"
    )

    def _watch() -> Optional[Dict]:
        try:
            for evt in w.stream(
                api.list_namespaced_event,
                namespace=namespace,
                field_selector=field_selector,
                timeout_seconds=0,
            ):
                obj = evt["object"]
                if getattr(obj, "reason", None) != "SuccessfulRescale":
                    continue

                scale_event_time = _extract_event_timestamp(obj)
                if scale_event_time is None:
                    continue

                if not_before and scale_event_time < not_before:
                    continue

                scale_event_time_iso = scale_event_time.isoformat()
                payload = {
                    "hpa_name": hpa_name,
                    "reason": obj.reason,
                    "scale_event_timestamp": scale_event_time_iso,
                }
                set_context("hpa_scale_event", payload)
                return payload

            return None
        finally:
            w.stop()

    return await asyncio.to_thread(_watch)


async def raise_on_pod_ready_after_hpa_scale(
    k8s: KubernetesClient,
    namespace: str,
    workload: WorkloadState,
) -> Optional[Dict[str, int]]:
    """
    Check if the deployment has reached the scaled ready replica target.

    Args:
        k8s: Kubernetes client instance.
        namespace: Kubernetes namespace of the deployment.
        workload: Workload state.

    Returns:
        Dict with replica counts if target is reached, otherwise None.

    Raises:
        HpaScalePodReadyError: If the deployment reaches the target ready replica count.
    """
    deployment = await k8s.read_namespaced_deployment(
        name=workload.spec.name,
        namespace=namespace,
    )

    start_replicas = workload.runtime.ready_replicas
    desired_replicas = deployment.status.replicas or 0
    ready_replicas = deployment.status.ready_replicas or 0

    # Optional guard: only treat this as scale-up readiness if the deployment
    # has actually scaled above the initial size.
    if start_replicas < desired_replicas <= ready_replicas:
        raise HpaScalePodReadyError(
            error_code="HPA_SCALED_PODS_READY",
            message="Scaled pods became Ready.",
            namespace=namespace,
            workload=workload.spec.name,
            context={
                "rule": (
                    "ready_replicas >= deployment_replicas and "
                    "deployment_replicas > initial_replicas"
                ),
                "inputs": {
                    "workload_name": workload.spec.name,
                    "namespace": namespace,
                },
                "observed": {
                    "before_replicas": start_replicas,
                    "desired_replicas": desired_replicas,
                    "ready_replicas": ready_replicas,
                },
            },
            retryable=False,
        )

    return None


async def raise_on_desired_replicas(
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
    deployment = await k8s.read_namespaced_deployment(
        name=workload_name,
        namespace=namespace,
    )
    desired_replicas = deployment.spec.replicas or 0
    ready_replicas = deployment.status.ready_replicas or 0
    selector_labels = deployment.spec.selector.match_labels or {}

    if ready_replicas >= desired_replicas:
        pods = await get_pods_by_labels(
            k8s=k8s,
            namespace=namespace,
            labels=selector_labels,
            pod_phase=None,
        )
        latest_pod_ready_time = get_latest_pod_ready_time(pods)
        latest_pod_ready_time_iso = (
            latest_pod_ready_time.isoformat()
            if latest_pod_ready_time is not None
            else None
        )
        raise ReachedDesiredReplicaError(
            error_code="DESIRED_REPLICA_COUNT_REACHED",
            message="Deployment has reached or exceeded the desired replica count.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "ready_replicas < desired_replicas",
                "inputs": {
                    "deployment": workload_name,
                    "namespace": namespace,
                },
                "observed": {
                    "ready_replicas": ready_replicas,
                    "desired_replicas": desired_replicas,
                    "at_or_above_desired": True,
                    "latest_pod_ready_time": latest_pod_ready_time_iso,
                },
            },
            fix_hint=(
                "Stop waiting for additional replicas. The deployment has already "
                "reached the desired state."
            ),
            retryable=False,
        )


async def raise_on_replicas_restored_cpu(
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

    deployment = await k8s.read_namespaced_deployment(
        name=initial_workload_state.spec.name, namespace=namespace
    )
    hpa = await k8s.read_namespaced_horizontal_pod_autoscaler(
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
            error_code="HPA_REPLICAS_RESTORED",
            message=(
                "Workload replicas have stabilized following CPU "
                "stress-induced scaling."
            ),
            namespace=namespace,
            workload=initial_workload_state.spec.name,
            context={
                "rule": (
                    "desired_replicas and ready_replicas fall below "
                    "stress peak replicas and CPU utilization decreases"
                ),
                "inputs": {
                    "deployment": initial_workload_state.spec.name,
                    "namespace": namespace,
                },
                "observed": {
                    "peak_replicas_during_stress": max_replicas_on_stress,
                    "current_ready_replicas": current_replicas,
                    "current_desired_replicas": desired_replicas,
                    "stress_average_utilization": stress_average_utilization,
                    "current_average_utilization": current_average_utilization,
                },
            },
            fix_hint=(
                "HPA scale-down stabilization detected. "
                "CPU stress recovery phase is complete."
            ),
            retryable=False,
        )
