import logging
import math
from typing import List, Optional

from kubernetes.client import V1Deployment, V1Node

from reslib import helpers as h
from reslib.constants import HpaMetricTypeEnum, HpaResourceNameEnum
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import (
    HpaMetricsNotFoundError,
    HpaNotConfiguredError,
    InsufficientMemoryError,
    PodsToStressExceededError,
    WorkloadAtMaxError,
)
from reslib.k8s.schema import HPAConfig, HPAMetricSpec, WorkloadState
from reslib.k8s.utils import (
    calculate_hpa_trigger,
    get_hpa_resource_metric,
    get_schedulable_nodes,
)

logger = logging.getLogger(__name__)


def validate_hpa_resource_metric(
    hpa: HPAConfig, metric_type: HpaMetricTypeEnum, resource: HpaResourceNameEnum
) -> HPAMetricSpec:
    """
    Validate that an HPA has a metric of the specified type and resource.

    Args:
        hpa: HPA configuration to check.
        metric_type: Type of metric to validate (e.g., RESOURCE).
        resource: Resource name (e.g., CPU or MEMORY).

    Returns:
        HPAMetricSpec corresponding to the metric.

    Raises:
        HpaMetricsNotFoundError: If the HPA does not define the requested
        metric/resource.
    """
    hpa_metric: Optional[HPAMetricSpec] = get_hpa_resource_metric(
        hpa=hpa, metric_type=metric_type, resource=resource
    )

    if hpa_metric is None:
        raise HpaMetricsNotFoundError(
            "Couldn't find HPA metric type",
            context={"metric_type": metric_type, "resource": resource.value},
        )

    return hpa_metric


def ensure_hpa_exists(workload: WorkloadState) -> HPAConfig:
    """
    Ensure that a workload has an HPA configured.

    Args:
        workload: Workload to check.

    Returns:
        The HPAConfig of the workload.

    Raises:
        HpaNotConfiguredError: If no HPA is configured for the workload.
    """
    if not workload.spec.hpa:
        raise HpaNotConfiguredError(
            "HPA is not configured for workload",
            context={"deployment": workload.spec.name},
        )
    return workload.spec.hpa


def ensure_not_at_max_replicas(workload: WorkloadState) -> None:
    """
    Ensure that the workload has not reached or exceeded its HPA max replicas.

    Args:
        workload: Workload to check.

    Raises:
        WorkloadAtMaxError: If ready replicas >= HPA max replicas.
    """
    ready = workload.status.ready_replicas
    max_replicas = workload.spec.hpa.max_replicas
    if ready >= max_replicas:
        raise WorkloadAtMaxError(
            "Workload is at max load.",
            context={"current_replicas": ready, "hpa_max": max_replicas},
        )


def validate_pods_to_stress_cpu(
    workload: WorkloadState,
    metric_type: HpaMetricTypeEnum,
    resource: HpaResourceNameEnum,
    idle_cpu_pct: int,
    max_cpu_stress_pct_per_pod: int,
    min_pods_idle_pct: int,
) -> int:
    """
    Calculate and validate how many pods need to be stressed to trigger HPA scale-up.

    Args:
        workload: Workload to stress.
        metric_type: Metric type (e.g., RESOURCE).
        resource: Resource to stress (CPU).
        idle_cpu_pct: Baseline idle CPU usage per pod.
        max_cpu_stress_pct_per_pod: Maximum CPU usage per stressed pod.
        min_pods_idle_pct: Minimum percentage of pods that must remain
                           idle (not stressed).

    Returns:
        Number of pods to stress.

    Raises:
        PodsToStressExceededError: If calculated pods to stress exceeds allowable limit.
    """
    hpa_metric = get_hpa_resource_metric(
        hpa=workload.spec.hpa, metric_type=metric_type, resource=resource
    )

    pods_to_stress, _ = calculate_hpa_trigger(
        workload=workload,
        metric=hpa_metric,
        idle_cpu_pct=idle_cpu_pct,
        max_cpu_stress_pct_per_pod=max_cpu_stress_pct_per_pod,
    )

    min_idle_pods_count = math.ceil(
        workload.status.ready_replicas * min_pods_idle_pct / 100
    )
    max_pods_can_stress = workload.status.ready_replicas - min_idle_pods_count

    if pods_to_stress > max_pods_can_stress:
        raise PodsToStressExceededError(
            "Total pods to stress exceeds the safety limit",
            context={
                "pods_to_stress": pods_to_stress,
                "required_idle_pods": min_idle_pods_count,
                "current_replicas": workload.status.ready_replicas,
            },
        )

    return pods_to_stress


def validate_can_deployment_scale(
    k8s: KubernetesClient,
    deployment: V1Deployment,
    replicas_to_add: int = 1,
) -> bool:
    """
    Check if the cluster has enough memory to scale the deployment by `replicas_to_add`.

    Args:
        k8s: Kubernetes client instance.
        deployment: Deployment object.
        replicas_to_add: Number of replicas you want to add.

    Returns:
        True if there is enough memory to schedule the additional replicas.

    Raises:
        InsufficientMemoryError: If there is not enough memory in any node.
    """
    # 1. Get the resource requests of the containers in the deployment
    total_mem_request_bytes = 0
    for c in deployment.spec.template.spec.containers:
        mem = c.resources.requests.get("memory", "0") if c.resources.requests else "0"
        total_mem_request_bytes += h.convert_to_bytes(mem)

    total_mem_request_bytes *= replicas_to_add

    # 2. Fetch all nodes and their allocatable memory
    nodes: List[V1Node] = get_schedulable_nodes(k8s=k8s, deployment=deployment)

    for node in nodes:
        alloc_mem = node.status.allocatable.get("memory", "0")
        alloc_bytes = h.convert_to_bytes(alloc_mem)
        if alloc_bytes >= total_mem_request_bytes:
            logger.info(
                f"Available memory: {alloc_bytes} "
                f"Required memory: {total_mem_request_bytes}"
            )
            return True

    raise InsufficientMemoryError(
        "Insufficient memory to scale deployment",
        context={
            "deployment": deployment.metadata.name,
            "additional_replicas": replicas_to_add,
            "available_memory": [
                node.status.allocatable.get("memory") for node in nodes
            ],
        },
    )
