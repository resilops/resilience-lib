import logging
import random
from typing import Dict

from reslib import helpers as h
from reslib.actions.schemas import EndpointDrainSchema
from reslib.core.context import get_context, set_context
from reslib.core.watchdog import watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.endpoints import (
    DRAINED_LABEL_VALUE_PREFIX,
    get_ready_service_pods,
    get_service_only_selector_key,
    pod_ip_absent_from_endpoint_slices,
    resolve_endpoint_drain_service,
)
from reslib.k8s.exceptions import (
    EndpointDrainSelectionError,
    EndpointDrainTimeoutError,
)
from reslib.k8s.schema import WorkloadState
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)


async def drain_pod_from_service_endpoints(**kwargs) -> Dict:
    """
    Remove one Ready pod from Service traffic by patching a Service-only label.

    The pod is not deleted or restarted. Only one label selected by the Service,
    but not by the owning workload, is changed so EndpointSlice membership drops
    for that pod while the workload continues running.
    """
    args = EndpointDrainSchema(**kwargs)
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    template_service_name = getattr(scenario.template, "service_name", None)
    k8s = KubernetesClient()

    service = await resolve_endpoint_drain_service(
        k8s=k8s,
        namespace=namespace,
        workload=workload,
        service_name=template_service_name,
    )
    service_name = service.metadata.name
    service_selector = service.spec.selector or {}
    service_only_label = get_service_only_selector_key(
        service_selector=service_selector,
        workload_selector=workload.spec.labels,
    )
    ready_pods = await get_ready_service_pods(
        k8s=k8s,
        namespace=namespace,
        service_selector=service_selector,
    )

    if len(ready_pods) < 2:
        raise EndpointDrainSelectionError(
            error_code="INSUFFICIENT_READY_ENDPOINTS",
            message=(
                f"Service '{service_name}' needs at least 2 Ready pod endpoints "
                f"for endpoint drain, but only {len(ready_pods)} were found."
            ),
            fix_hint=(
                "Scale the workload up or wait for at least two selected pods to "
                "be Ready before retrying."
            ),
        )

    pod = random.choice(ready_pods)
    pod_name = pod.metadata.name
    pod_ip = pod.status.pod_ip
    if not pod_ip:
        raise EndpointDrainSelectionError(
            error_code="POD_IP_MISSING",
            message=f"Selected pod '{pod_name}' does not have a pod IP.",
            fix_hint="Retry after pod networking is fully initialized.",
        )

    current_labels = dict(pod.metadata.labels or {})
    original_value = current_labels.get(service_only_label)
    if original_value != service_selector[service_only_label]:
        raise EndpointDrainSelectionError(
            error_code="SERVICE_ONLY_LABEL_MISMATCH",
            message=(
                f"Selected pod '{pod_name}' does not have expected Service-only "
                f"label '{service_only_label}'."
            ),
            fix_hint="Ensure the Service selector labels are present on selected pods.",
        )

    drained_at = h.utc_now_iso()
    drained_value = f"{DRAINED_LABEL_VALUE_PREFIX}-true"
    await k8s.patch_namespaced_pod(
        name=pod_name,
        namespace=namespace,
        body={"metadata": {"labels": {service_only_label: drained_value}}},
    )

    set_context(
        "endpoint_drain",
        {
            "namespace": namespace,
            "service_name": service_name,
            "pod_name": pod_name,
            "pod_ip": pod_ip,
            "label_key": service_only_label,
            "original_label_value": original_value,
            "drained_label_value": drained_value,
            "drained_at": drained_at,
        },
    )

    await watch_until(
        condition=pod_ip_absent_from_endpoint_slices,
        timeout=args.timeout_seconds,
        poll_interval=3,
        k8s=k8s,
        namespace=namespace,
        service_name=service_name,
        pod_ip=pod_ip,
        timeout_exception=EndpointDrainTimeoutError(
            error_code="ENDPOINT_DRAIN_TIMEOUT",
            message=(
                f"Pod '{pod_name}' IP '{pod_ip}' did not disappear from "
                f"EndpointSlices for Service '{service_name}' within "
                f"{args.timeout_seconds} seconds."
            ),
            fix_hint=(
                "Inspect EndpointSlices, Service selectors, and pod labels before "
                "retrying."
            ),
        ),
    )

    return {
        "result": "endpoint_drained",
        "reason": "One Ready pod was removed from Service EndpointSlices.",
        "observed": {
            "service_name": service_name,
            "pod_name": pod_name,
            "pod_ip": pod_ip,
            "drained_label": service_only_label,
            "endpoint_drained_at": drained_at,
        },
    }
