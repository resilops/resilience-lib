import logging

from reslib import helpers as h
from reslib.core.context import get_context
from reslib.core.watchdog import watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.endpoints import pod_ip_present_in_endpoint_slices
from reslib.k8s.exceptions import EndpointRestoreTimeoutError
from reslib.rollbacks.schemas import EndpointRestoreTimeout

logger = logging.getLogger(__name__)


async def restore_pod_to_service_endpoints(**kwargs):
    """
    Restore the Service-only label changed during endpoint drain.

    This returns the pod to Service EndpointSlices without restarting or replacing
    it, then waits until the pod IP appears in EndpointSlices again.
    """
    args = EndpointRestoreTimeout(**kwargs)
    drain = get_context("endpoint_drain")
    k8s = KubernetesClient()

    await k8s.patch_namespaced_pod(
        name=drain["pod_name"],
        namespace=drain["namespace"],
        body={
            "metadata": {
                "labels": {
                    drain["label_key"]: drain["original_label_value"],
                }
            }
        },
    )

    await watch_until(
        condition=pod_ip_present_in_endpoint_slices,
        timeout=args.timeout_seconds,
        poll_interval=3,
        k8s=k8s,
        namespace=drain["namespace"],
        service_name=drain["service_name"],
        pod_ip=drain["pod_ip"],
        timeout_exception=EndpointRestoreTimeoutError(
            error_code="ENDPOINT_RESTORE_TIMEOUT",
            message=(
                f"Pod '{drain['pod_name']}' IP '{drain['pod_ip']}' did not return "
                f"to EndpointSlices for Service '{drain['service_name']}' within "
                f"{args.timeout_seconds} seconds."
            ),
            fix_hint=(
                "Inspect the pod label, Service selector, EndpointSlices, and pod "
                "readiness state."
            ),
        ),
    )

    restored_at = h.utc_now_iso()
    logger.info("Endpoint drain restored")
    return {
        "result": "endpoint_restored",
        "status": "success",
        "reason": "The drained pod label was restored and the pod IP returned.",
        "observed": {
            "service_name": drain["service_name"],
            "pod_name": drain["pod_name"],
            "pod_ip": drain["pod_ip"],
            "restored_label": drain["label_key"],
            "endpoint_restored_at": restored_at,
        },
    }
