from kubernetes.client.rest import ApiException

from reslib.core.context import get_context
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import MetricsServerUnavailableError
from reslib.k8s.pods import get_workload_pods
from reslib.k8s.schema import WorkloadState
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

    k8s = KubernetesClient()
    pods = await get_workload_pods(
        k8s=k8s, namespace=namespace, workload_spec=workload.spec
    )
    pod_name = pods[0].metadata.name

    try:
        metrics = await k8s.get_namespaced_custom_object(
            group="metrics.k8s.io",
            version="v1beta1",
            namespace=namespace,
            plural="pods",
            name=pod_name,
        )
    except ApiException as e:
        raise MetricsServerUnavailableError(
            error_code="METRICS_SERVER_API_ERROR",
            message=(
                f"Unable to read metrics for pod '{pod_name}' in namespace "
                f"'{namespace}' from the Kubernetes Metrics API."
            ),
            fix_hint=(
                "Verify that Metrics Server is installed and healthy, and that the "
                "caller can read pod metrics in this namespace."
            ),
        ) from e
    except Exception as e:
        raise MetricsServerUnavailableError(
            error_code="METRICS_SERVER_QUERY_UNEXPECTED_ERROR",
            message=(
                f"Unable to query metrics for pod '{pod_name}' in namespace "
                f"'{namespace}'."
            ),
            fix_hint=(
                "Check cluster connectivity and Kubernetes client configuration. "
                f"The last error was: {type(e).__name__}: {e}"
            ),
        ) from e

    if not metrics.get("containers"):
        raise MetricsServerUnavailableError(
            error_code="METRICS_SERVER_EMPTY_CONTAINER_METRICS",
            message=(
                f"The metrics API returned no container metrics for pod "
                f"'{pod_name}' in namespace '{namespace}'."
            ),
            fix_hint=(
                "Ensure the pod is running and producing metrics, and that "
                "Metrics Server is collecting data for this namespace and node."
            ),
        )
