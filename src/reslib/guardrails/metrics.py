from kubernetes.client.rest import ApiException

from reslib.core.context import get_context
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import MetricsServerUnavailableError
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import get_workload_pods
from reslib.schemas.scenario import ResiliencyScenario


async def ensure_metrics_server_available() -> None:
    """
    Validate that the Kubernetes Metrics Server is reachable and returns
    pod metrics for the target workload.

    This guardrail queries the metrics API for a representative pod from
    the workload to confirm that:

    - The metrics API can be accessed in the workload namespace.
    - The response includes container metrics for the pod.

    Raises:
        MetricsServerUnavailableError:
            If the metrics API is unreachable, returns an error, or
            provides no container metrics for the pod.
    """

    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace
    workload_name: str = scenario.template.workload

    k8s = KubernetesClient()
    pods = get_workload_pods(k8s=k8s, namespace=namespace, workload_spec=workload.spec)
    pod_name = pods[0].metadata.name

    try:
        metrics = k8s.custom.get_namespaced_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="pods",
            name=pod_name,
        )
    except ApiException as e:
        # Catch forbidden, not found, or other API errors
        raise MetricsServerUnavailableError(
            error_code="METRICS_SERVER_API_ERROR",
            message="Failed to query pod metrics from metrics.k8s.io API.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "metrics.k8s.io API is reachable and authorized",
                "inputs": {
                    "namespace": namespace,
                    "pod_name": pod_name,
                    "api": "metrics.k8s.io/v1beta1/pods",
                },
                "observed": {
                    "status": getattr(e, "status", None),
                    "reason": getattr(e, "reason", None),
                    "body": getattr(e, "body", None),
                },
            },
            fix_hint=(
                "Verify Metrics Server is installed and healthy, and ensure "
                "the caller has RBAC "
                "permission to read pod metrics (get/list on metrics.k8s.io pods)."
            ),
            retryable=False,
        ) from e
    except Exception as e:
        raise MetricsServerUnavailableError(
            error_code="METRICS_SERVER_QUERY_UNEXPECTED_ERROR",
            message="Unexpected error occurred while querying pod metrics.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "metrics query succeeds without unexpected exceptions",
                "inputs": {
                    "namespace": namespace,
                    "pod_name": pod_name,
                    "api": "metrics.k8s.io/v1beta1/pods",
                },
                "observed": {
                    "exception_type": type(e).__name__,
                    "exception_message": str(e),
                },
            },
            fix_hint=(
                "Check cluster connectivity and client configuration. "
                "If this persists, inspect logs for the component issuing "
                "the metrics request."
            ),
            retryable=False,
        ) from e

    if not metrics.get("containers"):
        raise MetricsServerUnavailableError(
            error_code="METRICS_SERVER_EMPTY_CONTAINER_METRICS",
            message=(
                "Metrics response contains no container metrics for the selected pod."
            ),
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "metrics response includes non-empty `containers` list",
                "inputs": {
                    "namespace": namespace,
                    "pod_name": pod_name,
                    "workload_name": workload.spec.name,
                },
                "observed": {
                    "containers_present": bool(metrics.get("containers")),
                    "metrics_keys": sorted(list(metrics.keys())),
                },
            },
            fix_hint=(
                "Ensure the pod is running and producing metrics, and that "
                "Metrics Server is "
                "collecting metrics for this namespace and node."
            ),
            retryable=True,
        )
