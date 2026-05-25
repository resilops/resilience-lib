from typing import Dict, List, Optional

from kubernetes.client import V1Pod

from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import EndpointDrainSelectionError
from reslib.k8s.pods import get_pods_by_labels, is_pod_ready
from reslib.k8s.schema import WorkloadState

SERVICE_NAME_LABEL = "kubernetes.io/service-name"
DRAINED_LABEL_VALUE_PREFIX = "resilty-drained"


def labels_to_selector(labels: Dict[str, str]) -> str:
    """Convert a label mapping to a Kubernetes label selector."""
    return ",".join(f"{key}={value}" for key, value in labels.items())


def get_service_only_selector_key(
    *,
    service_selector: Dict[str, str],
    workload_selector: Optional[Dict[str, str]],
) -> str:
    """Return a Service selector key that is not part of the workload selector."""
    workload_selector = workload_selector or {}
    for key in sorted(service_selector):
        if key not in workload_selector:
            return key

    raise EndpointDrainSelectionError(
        error_code="NO_SERVICE_ONLY_SELECTOR_LABEL",
        message=(
            "The Service selector does not contain a label that is separate from "
            "the workload selector, so draining one pod by label could affect "
            "workload ownership."
        ),
        fix_hint=(
            "Add a Service-only selector label to the pod template and Service "
            "before running endpoint drain."
        ),
    )


async def resolve_endpoint_drain_service(
    *,
    k8s: KubernetesClient,
    namespace: str,
    workload: WorkloadState,
    service_name: Optional[str] = None,
):
    """Read the Service used for endpoint drain."""
    resolved_name = service_name or workload.spec.service_name
    if not resolved_name:
        raise EndpointDrainSelectionError(
            error_code="SERVICE_NOT_RESOLVED",
            message=(
                f"Workload '{workload.spec.name}' does not have a resolved Service "
                "for endpoint drain."
            ),
            fix_hint=(
                "Provide a service_name in the scenario template or ensure "
                "discovery can associate a Service with this workload."
            ),
        )

    service = await k8s.read_namespaced_service(name=resolved_name, namespace=namespace)
    if not service.spec.selector:
        raise EndpointDrainSelectionError(
            error_code="SERVICE_SELECTOR_MISSING",
            message=f"Service '{resolved_name}' does not have a selector.",
            fix_hint="Use a selector-based Service for endpoint drain.",
        )
    return service


async def get_ready_service_pods(
    *,
    k8s: KubernetesClient,
    namespace: str,
    service_selector: Dict[str, str],
) -> List[V1Pod]:
    """Return Ready pods selected by a Service selector."""
    pods = await get_pods_by_labels(
        k8s=k8s,
        namespace=namespace,
        labels=service_selector,
        pod_phase=None,
    )
    return [pod for pod in pods if is_pod_ready(pod)]


async def endpoint_slice_contains_ip(
    *,
    k8s: KubernetesClient,
    namespace: str,
    service_name: str,
    pod_ip: str,
) -> bool:
    """Return whether any EndpointSlice for a Service contains the pod IP."""
    slices = await k8s.list_namespaced_endpoint_slice(
        namespace=namespace,
        label_selector=f"{SERVICE_NAME_LABEL}={service_name}",
    )
    for endpoint_slice in slices.items:
        for endpoint in endpoint_slice.endpoints or []:
            if pod_ip in (endpoint.addresses or []):
                return True
    return False


async def pod_ip_absent_from_endpoint_slices(**kwargs) -> bool:
    """Return whether a pod IP has disappeared from EndpointSlices."""
    return not await endpoint_slice_contains_ip(**kwargs)


async def pod_ip_present_in_endpoint_slices(**kwargs) -> bool:
    """Return whether a pod IP is present in EndpointSlices."""
    return await endpoint_slice_contains_ip(**kwargs)
