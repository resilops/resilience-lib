import asyncio
import logging
import math
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

from kubernetes.client import V2HorizontalPodAutoscaler

from reslib import helpers as h
from reslib.constants import HpaMetricSourceEnum, HpaResourceTypeEnum
from reslib.core.context import get_context, set_context
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import (
    HpaNotConfiguredError,
    HpaScalePodReadyError,
    ReachedDesiredReplicaError,
    ReplicasRestoredError,
)
from reslib.k8s.pods import get_latest_pod_ready_time, get_pods_by_labels
from reslib.k8s.schema import (
    HPAConfig,
    HPAMetricSpec,
    WorkloadRuntimeState,
    WorkloadState,
)

logger = logging.getLogger(__name__)

HPA_SCALE_UP_EVENT_CONTEXT_KEY = "hpa_scale_up_event"
HPA_SCALE_DOWN_EVENT_CONTEXT_KEY = "hpa_scale_down_event"
HPA_SCALED_PODS_READY_CONTEXT_KEY = "hpa_scaled_pods_ready"
DESIRED_REPLICA_REACHED_CONTEXT_KEY = "desired_replica_reached"
REPLICAS_RESTORED_CONTEXT_KEY = "replicas_restored"


def get_hpa_resource_metric(
    hpa: HPAConfig,
    metric_source: HpaMetricSourceEnum,
    resource_type: HpaResourceTypeEnum,
) -> Optional[HPAMetricSpec]:
    """Return the HPA metric matching the given metric type and resource."""
    for metric in hpa.metrics:
        if (
            metric.type == metric_source
            and metric.resource.get("name") == resource_type.value
        ):
            return metric
    return None


def calculate_hpa_trigger(
    status: WorkloadRuntimeState,
    metric: HPAMetricSpec,
    idle_cpu_pct: int,
    cpu_stress_threshold_pct: Optional[int] = 95,
) -> Tuple[int, int]:
    """Compute the minimum pods and CPU load needed to trigger HPA scale-up."""
    scenario = get_context("scenario")
    target: dict = metric.resource.get("target", {})
    average_utilization = target.get("average_utilization")

    if average_utilization is None:
        raise HpaNotConfiguredError(
            error_code="HPA_AVERAGE_UTILIZATION_MISSING",
            message=(
                f"The HPA metric for workload '{scenario.template.workload}' is "
                "missing `target.averageUtilization`."
            ),
            fix_hint=(
                "Configure the HPA resource metric with "
                "`target.averageUtilization` before running CPU stress tests."
            ),
        )

    replicas = status.ready_replicas
    total_required_cpu_increase = replicas * (average_utilization - idle_cpu_pct)

    for pods_to_stress in range(1, replicas + 1):
        stress_percent = idle_cpu_pct + total_required_cpu_increase / pods_to_stress
        stress_percent = math.ceil(stress_percent)
        if stress_percent <= cpu_stress_threshold_pct:
            return pods_to_stress, stress_percent

    return replicas, cpu_stress_threshold_pct


def get_hpa_current_average_utilization(
    hpa: V2HorizontalPodAutoscaler,
) -> Optional[int]:
    """Extract average CPU utilization from autoscaling/v2 HPA status metrics."""
    for metric in hpa.status.current_metrics or []:
        if metric.type == HpaMetricSourceEnum.RESOURCE.value and metric.resource:
            return metric.resource.current.average_utilization
    return None


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


def _build_hpa_scale_event_payload(
    *,
    hpa_name: str,
    event_obj: Any,
    desired_replicas: Optional[int],
    event_timestamp: datetime,
) -> Dict[str, Any]:
    """Build a normalized HPA scale event payload for runtime context."""
    return {
        "hpa_name": hpa_name,
        "event_reason": event_obj.reason,
        "event_message": getattr(event_obj, "message", None),
        "desired_replicas": desired_replicas,
        "scale_event_timestamp": event_timestamp.isoformat(),
    }


async def _watch_hpa_scale_event(
    *,
    k8s: KubernetesClient,
    namespace: str,
    workload: WorkloadState,
    context_key: str,
    not_before: Optional[datetime],
    event_matches: Callable[[Optional[int]], bool],
) -> Optional[Dict[str, Any]]:
    """Watch for a matching HPA SuccessfulRescale event and store it in context."""
    hpa_name = workload.spec.hpa.name
    api = k8s.new_v1_api()
    watcher = k8s.new_watch()
    field_selector = (
        f"involvedObject.kind=HorizontalPodAutoscaler,"
        f"involvedObject.name={hpa_name},"
        f"reason=SuccessfulRescale"
    )

    def _watch() -> Optional[Dict]:
        try:
            for event in watcher.stream(
                api.list_namespaced_event,
                namespace=namespace,
                field_selector=field_selector,
            ):
                obj = event["object"]
                scale_event_time = _extract_event_timestamp(obj)
                if scale_event_time is None:
                    continue
                if not_before and scale_event_time < not_before:
                    continue

                hpa = k8s.autoscaling.read_namespaced_horizontal_pod_autoscaler(
                    name=hpa_name,
                    namespace=namespace,
                )
                desired_replicas = hpa.status.desired_replicas
                if not event_matches(desired_replicas):
                    continue

                payload = _build_hpa_scale_event_payload(
                    hpa_name=hpa_name,
                    event_obj=obj,
                    desired_replicas=desired_replicas,
                    event_timestamp=scale_event_time,
                )
                set_context(context_key, payload)
                return payload

            return None
        except Exception:
            logger.exception("Failed to watch HPA scale event")
            raise
        finally:
            watcher.stop()

    return await asyncio.to_thread(_watch)


async def wait_for_hpa_scale_up_event(
    *,
    k8s: KubernetesClient,
    namespace: str,
    workload: WorkloadState,
    not_before: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Watch for and store the HPA scale-up event for the current workload."""
    initial_replicas = workload.runtime.ready_replicas
    return await _watch_hpa_scale_event(
        k8s=k8s,
        namespace=namespace,
        workload=workload,
        context_key=HPA_SCALE_UP_EVENT_CONTEXT_KEY,
        not_before=not_before,
        event_matches=lambda desired_replicas: (
            desired_replicas is not None and desired_replicas > initial_replicas
        ),
    )


async def wait_for_hpa_scale_down_event(
    *,
    k8s: KubernetesClient,
    namespace: str,
    workload: WorkloadState,
    peak_replicas: int,
    not_before: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Watch for and store the HPA scale-down event after a stress peak."""
    return await _watch_hpa_scale_event(
        k8s=k8s,
        namespace=namespace,
        workload=workload,
        context_key=HPA_SCALE_DOWN_EVENT_CONTEXT_KEY,
        not_before=not_before,
        event_matches=lambda desired_replicas: (
            desired_replicas is not None and desired_replicas < peak_replicas
        ),
    )


async def raise_on_scaled_pods_ready(
    k8s: KubernetesClient,
    namespace: str,
    workload: WorkloadState,
) -> Optional[Dict[str, int]]:
    """Raise once scaled pods are ready above the initial replica count."""
    deployment = await k8s.read_namespaced_deployment(
        name=workload.spec.name,
        namespace=namespace,
    )
    start_replicas = workload.runtime.ready_replicas
    desired_replicas = deployment.status.replicas or 0
    ready_replicas = deployment.status.ready_replicas or 0
    selector_labels = deployment.spec.selector.match_labels or {}

    if start_replicas < desired_replicas <= ready_replicas:
        hpa = await k8s.read_namespaced_horizontal_pod_autoscaler(
            name=workload.spec.hpa.name, namespace=namespace
        )
        pods = await get_pods_by_labels(
            k8s=k8s,
            namespace=namespace,
            labels=selector_labels,
            pod_phase=None,
        )
        latest_pod_ready_time = get_latest_pod_ready_time(pods)
        set_context(
            HPA_SCALED_PODS_READY_CONTEXT_KEY,
            {
                "before_replicas": start_replicas,
                "desired_replicas": desired_replicas,
                "ready_replicas": ready_replicas,
                "latest_pod_ready_time": latest_pod_ready_time,
                "average_utilization": get_hpa_current_average_utilization(hpa=hpa),
            },
        )
        raise HpaScalePodReadyError(
            error_code="HPA_SCALED_PODS_READY",
            message=(
                f"Workload '{workload.spec.name}' scaled from {start_replicas} to "
                f"{ready_replicas} ready replica(s)."
            ),
        )

    return None


async def raise_on_desired_replicas(
    k8s: KubernetesClient,
    workload_name: str,
    namespace: str,
    after: str,
) -> None:
    """Raise when a deployment reaches or exceeds its desired replica count."""
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

        # Prevent false positives from stale deployment status
        # or previously-ready pods.
        if (
            latest_pod_ready_time is None
            or latest_pod_ready_time <= datetime.fromisoformat(after)
        ):
            return

        set_context(
            DESIRED_REPLICA_REACHED_CONTEXT_KEY,
            {
                "ready_replicas": ready_replicas,
                "desired_replicas": desired_replicas,
                "at_or_above_desired": True,
                "latest_pod_ready_time": (
                    latest_pod_ready_time.isoformat()
                    if latest_pod_ready_time is not None
                    else None
                ),
            },
        )
        raise ReachedDesiredReplicaError(
            error_code="DESIRED_REPLICA_COUNT_REACHED",
            message=(
                f"Deployment '{workload_name}' now has {ready_replicas} ready "
                f"replica(s), which meets the desired count of {desired_replicas}."
            ),
            fix_hint=("No action is required. The deployment has already recovered."),
        )


async def raise_on_replicas_restored(
    k8s: KubernetesClient,
    namespace: str,
    stress_context: Dict[Any, Any],
) -> None:
    """Raise once replicas and utilization settle below the stress peak."""
    initial_workload_state: WorkloadState = stress_context.get("workload")
    stress_average_utilization = stress_context.get("average_utilization")
    max_replicas_on_stress = stress_context.get("ready_replicas")

    deployment = await k8s.read_namespaced_deployment(
        name=initial_workload_state.spec.name,
        namespace=namespace,
    )
    hpa = await k8s.read_namespaced_horizontal_pod_autoscaler(
        name=initial_workload_state.spec.hpa.name,
        namespace=namespace,
    )

    current_replicas = deployment.status.ready_replicas or 0
    desired_replicas = hpa.status.desired_replicas or 0

    if max_replicas_on_stress > desired_replicas == current_replicas:
        current_average_utilization = get_hpa_current_average_utilization(hpa)
        set_context(
            REPLICAS_RESTORED_CONTEXT_KEY,
            {
                "peak_replicas_during_stress": max_replicas_on_stress,
                "current_ready_replicas": current_replicas,
                "current_desired_replicas": desired_replicas,
                "stress_average_utilization": stress_average_utilization,
                "current_average_utilization": current_average_utilization,
                "scale_down_completed_at": h.utc_now_iso(),
            },
        )
        raise ReplicasRestoredError(
            error_code="HPA_REPLICAS_RESTORED",
            message=(
                f"Workload '{initial_workload_state.spec.name}' has returned to "
                f"{current_replicas} ready replica(s) after HPA scale-up."
            ),
            fix_hint="No action is required. The HPA recovery phase has completed.",
        )
