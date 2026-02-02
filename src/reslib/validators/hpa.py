from typing import Optional

from reslib.constants import HpaMetricTypeEnum, HpaResourceNameEnum
from reslib.k8s.exceptions import (
    HpaMetricsNotFoundError,
    HpaNotConfiguredError,
    PodsToStressExceededError,
    WorkloadAtMaxError,
)
from reslib.k8s.schema import HPAConfig, HPAMetricSpec, WorkloadState
from reslib.k8s.utils import calculate_hpa_trigger, get_hpa_resource_metric


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
            f"Couldn't find HPA metric type: {metric_type.value} "
            f"and resource: {resource.value}"
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
            f"HPA is not configured for workload '{workload.spec.name}'"
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
            f"Workload has reached/exceeded its HPA maximum replica count "
            f"(ready_replicas={ready}, max_replicas={max_replicas})"
        )


def validate_pods_to_stress_cpu(
    workload: WorkloadState,
    metric_type: HpaMetricTypeEnum,
    resource: HpaResourceNameEnum,
    idle_cpu_pct: int,
    max_cpu_stress_pct_per_pod: int,
) -> int:
    """
    Calculate and validate how many pods need to be stressed to trigger HPA scale-up.

    Args:
        workload: Workload to stress.
        metric_type: Metric type (e.g., RESOURCE).
        resource: Resource to stress (CPU).
        idle_cpu_pct: Baseline idle CPU usage per pod.
        max_cpu_stress_pct_per_pod: Target CPU usage per stressed pod.

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

    max_pods_can_stress = max(workload.status.ready_replicas, 0)
    if pods_to_stress > max_pods_can_stress:
        raise PodsToStressExceededError(
            f"Calculated pods to stress ({pods_to_stress}) exceeds limit "
            f"(ready_replicas={workload.status.ready_replicas}, min_idle={min_idle})"
        )

    return pods_to_stress
