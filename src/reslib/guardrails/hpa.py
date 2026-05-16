import logging
import math
from typing import Optional

from reslib.constants import SUPPORTED_HPA_METRIC_SOURCES, SUPPORTED_HPA_RESOURCE_TYPES
from reslib.core.context import get_context
from reslib.exceptions import NotSupportedError
from reslib.k8s.exceptions import (
    HpaMetricsNotFoundError,
    HpaNotConfiguredError,
    PodsToStressExceededError,
    WorkloadAtMaxError,
)
from reslib.k8s.scaling import calculate_hpa_trigger, get_hpa_resource_metric
from reslib.k8s.schema import HPAMetricSpec, WorkloadState
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)


async def validate_metric_and_resource() -> None:
    """
    Validate that the requested HPA metric source and resource type are supported.

    This is a guardrail that blocks unsupported combinations early, before any
    HPA inspection or scaling logic runs.

    Raises:
        BaseError:
            If the metric source or resource type is not in the supported sets.
    """
    scenario: ResiliencyScenario = get_context("scenario")

    if scenario.template.metric_source not in SUPPORTED_HPA_METRIC_SOURCES:
        raise NotSupportedError(
            error_code="HPA_METRIC_SOURCE_NOT_SUPPORTED",
            message=(
                f"Metric source '{scenario.template.metric_source.value}' is not "
                "supported for this HPA scaling scenario."
            ),
            fix_hint=(
                "Use one of the supported metric sources: "
                f"{', '.join(m.value for m in SUPPORTED_HPA_METRIC_SOURCES)}."
            ),
        )

    if scenario.template.resource_type not in SUPPORTED_HPA_RESOURCE_TYPES:
        raise NotSupportedError(
            error_code="HPA_RESOURCE_TYPE_NOT_SUPPORTED",
            message=(
                f"Resource type '{scenario.template.resource_type.value}' is not "
                "supported for this HPA scaling scenario."
            ),
            fix_hint=(
                "Use one of the supported resource types: "
                f"{', '.join(r.value for r in SUPPORTED_HPA_RESOURCE_TYPES)}."
            ),
        )


async def validate_hpa_resource_metric() -> None:
    """
    Validate that the workload HPA defines the requested metric and resource type.

    This function parses `HPAResourceMetricSchema` from kwargs, then searches the
    workload's HPA spec for a matching resource metric (e.g., CPU or memory).

    Raises:
        BaseError:
            If the workload does not have an HPA configured or if the requested
            metric/resource type is not present in the HPA spec.
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    workload_name = scenario.template.workload

    hpa_metric: Optional[HPAMetricSpec] = get_hpa_resource_metric(
        hpa=workload.spec.hpa,
        metric_source=scenario.template.metric_source,
        resource_type=scenario.template.resource_type,
    )

    if hpa_metric is None:
        raise HpaMetricsNotFoundError(
            error_code="HPA_METRIC_NOT_FOUND",
            message=(
                f"The HPA for workload '{workload_name}' does not define a "
                f"{scenario.template.metric_source.value}/"
                f"{scenario.template.resource_type.value} metric."
            ),
            fix_hint=(
                "Update the HPA to include the requested resource metric, "
                "or choose a metric that already exists on the workload."
            ),
        )

    return None


async def ensure_hpa_exists() -> None:
    """
    Ensure that a workload has an HPA configured.

    Raises:
        HpaNotConfiguredError: If no HPA is configured for the workload.
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload
    if not workload.spec.hpa:
        raise HpaNotConfiguredError(
            error_code="HPA_NOT_CONFIGURED",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' does not "
                "have an HPA configured."
            ),
            fix_hint=(
                "Configure an HPA for this workload before running HPA-based tests."
            ),
        )
    return None


async def ensure_hpa_not_at_max_replicas() -> None:
    """
    Validate that the workload is not already at the HPA maximum replicas.

    If the workload has no HPA configured, this guardrail is a no-op.
    Otherwise, it compares the current ready replicas to the HPA maximum
    and blocks further disruption if the workload is already at max.

    Raises:
        WorkloadAtMaxError:
            If the workload is already at the HPA maximum replica count.
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    workload_name = scenario.template.workload
    if not workload.spec.hpa:
        return None

    ready = workload.runtime.ready_replicas
    max_replicas = workload.spec.hpa.max_replicas
    if ready >= max_replicas:
        raise WorkloadAtMaxError(
            error_code="WORKLOAD_AT_HPA_MAX_REPLICAS",
            message=(
                f"Workload '{workload_name}' is already at {ready} ready replica(s), "
                f"which meets or exceeds the HPA maximum of {max_replicas}."
            ),
            fix_hint="Reduce load or increase `hpa.maxReplicas` before retrying.",
        )
    return None


async def validate_pods_to_stress_cpu() -> None:
    """
    Validate that the computed number of pods to stress stays within
    the configured safety limits.

    This guardrail uses HPA metrics and CPU thresholds to determine how
    many pods should be stressed, then ensures that enough pods remain
    idle based on the configured minimum idle percentage.

    Raises:
        PodsToStressExceededError:
            If the calculated number of pods to stress exceeds the safety
            limit derived from the minimum idle pod requirement.
    """

    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")

    hpa_metric = get_hpa_resource_metric(
        hpa=workload.spec.hpa,
        metric_source=scenario.template.metric_source,
        resource_type=scenario.template.resource_type,
    )

    pods_to_stress, _ = calculate_hpa_trigger(
        status=workload.runtime,
        metric=hpa_metric,
        idle_cpu_pct=scenario.template.idle_cpu_pct,
        cpu_stress_threshold_pct=scenario.template.cpu_stress_threshold_pct,
    )

    min_idle_pods_count = math.ceil(
        workload.runtime.ready_replicas * scenario.template.min_idle_pct / 100
    )
    max_pods_can_stress = workload.runtime.ready_replicas - min_idle_pods_count

    if pods_to_stress > max_pods_can_stress:
        raise PodsToStressExceededError(
            error_code="PODS_TO_STRESS_EXCEEDS_IDLE_SAFETY_LIMIT",
            message=(
                f"The scenario needs to stress {pods_to_stress} pod(s), but only "
                f"{max_pods_can_stress} can be stressed while keeping "
                f"{min_idle_pods_count} idle pod(s)."
            ),
            fix_hint=(
                "Lower the stress target, lower `min_idle_pct`, or scale the "
                "workload up before retrying."
            ),
        )

    return None
