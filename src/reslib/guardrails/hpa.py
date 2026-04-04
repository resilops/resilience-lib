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
from reslib.k8s.schema import HPAMetricSpec, WorkloadState
from reslib.k8s.utils import (
    calculate_hpa_trigger,
    get_hpa_resource_metric,
)
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
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload

    if scenario.template.metric_source not in SUPPORTED_HPA_METRIC_SOURCES:
        raise NotSupportedError(
            error_code="HPA_METRIC_SOURCE_NOT_SUPPORTED",
            message="Requested HPA metric source is not supported for scaling tests.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "metric_source in SUPPORTED_HPA_METRIC_SOURCES",
                "inputs": {
                    "metric_source": scenario.template.metric_source.value,
                    "resource_type": scenario.template.resource_type.value,
                },
                "observed": {
                    "metric_source": scenario.template.metric_source.value,
                    "supported_metric_sources": [
                        m.value for m in SUPPORTED_HPA_METRIC_SOURCES
                    ],
                },
            },
            fix_hint=(
                "Choose a supported `metric_source` from `supported_metric_sources`, "
                "or implement support for this metric source."
            ),
            retryable=False,
        )

    if scenario.template.resource_type not in SUPPORTED_HPA_RESOURCE_TYPES:
        raise NotSupportedError(
            error_code="HPA_RESOURCE_TYPE_NOT_SUPPORTED",
            message="Requested HPA resource type is not supported for scaling tests.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "resource_type in SUPPORTED_HPA_RESOURCE_TYPES",
                "inputs": {
                    "metric_source": scenario.template.metric_source.value,
                    "resource_type": scenario.template.resource_type.value,
                },
                "observed": {
                    "resource_type": scenario.template.resource_type.value,
                    "supported_resource_types": [
                        r.value for r in SUPPORTED_HPA_RESOURCE_TYPES
                    ],
                },
            },
            fix_hint=(
                "Choose a supported `resource_type` from `supported_resource_types`, "
                "or implement support for this resource type."
            ),
            retryable=False,
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
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload

    hpa_metric: Optional[HPAMetricSpec] = get_hpa_resource_metric(
        hpa=workload.spec.hpa,
        metric_source=scenario.template.metric_source,
        resource_type=scenario.template.resource_type,
    )

    if hpa_metric is None:
        raise HpaMetricsNotFoundError(
            error_code="HPA_METRIC_NOT_FOUND",
            message="Requested HPA metric was not found in the HPA specification.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "HPA defines a metric matching (metric_source, resource_type)",
                "inputs": {
                    "metric_source": scenario.template.metric_source.value,
                    "resource_type": scenario.template.resource_type.value,
                },
                "observed": {
                    "match_found": False,
                },
            },
            fix_hint=(
                "Update the HPA to include the requested resource metric, "
                "or change `metric_source` / `resource_type` to one that exists."
            ),
            retryable=False,
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
            message="Workload does not have an HPA configuration.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "workload.spec.hpa is None",
                "inputs": {},
                "observed": {"hpa_present": False},
            },
            fix_hint=(
                "Enable/configure an HPA for this workload before "
                "validating HPA metrics."
            ),
            retryable=False,
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
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload
    if not workload.spec.hpa:
        return None

    ready = workload.runtime.ready_replicas
    max_replicas = workload.spec.hpa.max_replicas
    if ready >= max_replicas:
        raise WorkloadAtMaxError(
            error_code="WORKLOAD_AT_HPA_MAX_REPLICAS",
            message="Workload is already at or above the HPA maximum replicas.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "ready_replicas < hpa.max_replicas",
                "inputs": {
                    "hpa_max_replicas": max_replicas,
                },
                "observed": {
                    "ready_replicas": ready,
                    "at_or_above_max": True,
                },
                "required": {
                    "hpa_max_replicas": max_replicas,
                },
            },
            fix_hint=(
                "Reduce load or increase `hpa.maxReplicas` before "
                "running this operation, because the workload cannot scale up further."
            ),
            retryable=True,
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
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload

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
            message="Calculated number of pods to stress exceeds allowed safety limit.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "pods_to_stress <= " "ready_replicas - required_idle_pods",
                "inputs": {
                    "idle_cpu_pct": scenario.template.idle_cpu_pct,
                    "cpu_stress_threshold_pct": (
                        scenario.template.cpu_stress_threshold_pct
                    ),
                    "min_idle_pct": scenario.template.min_idle_pct,
                    "metric_source": scenario.template.metric_source.value,
                    "resource_type": scenario.template.resource_type.value,
                },
                "observed": {
                    "ready_replicas": workload.runtime.ready_replicas,
                    "pods_to_stress": pods_to_stress,
                    "required_idle_pods": min_idle_pods_count,
                },
                "required": {
                    "max_allowed_pods_to_stress": max_pods_can_stress,
                },
            },
            fix_hint=(
                "Increase `min_idle_pct`, reduce "
                "`cpu_stress_threshold_pct`, or scale the workload "
                "to allow more pods to remain idle."
            ),
            retryable=True,
        )

    return None
