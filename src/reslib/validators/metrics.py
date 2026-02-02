from typing import List

from kubernetes.client import V1Deployment, V1Pod
from kubernetes.client.rest import ApiException

from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import MetricsServerUnavailableError
from reslib.k8s.utils import get_deployment_pods


def ensure_metrics_server_available(
    k8s: KubernetesClient, deployment: V1Deployment, namespace: str
) -> List[V1Pod]:
    """
    Ensure that the Kubernetes Metrics Server is available and reporting metrics
    for the pods of a given deployment.

    HPA scaling depends on metrics (CPU/memory/custom). If metrics are not
    available, HPA will not scale and scaling experiments may fail.

    Args:
        k8s: Kubernetes client instance.
        deployment: Deployment to validate.
        namespace: Namespace of the deployment.

    Returns:
        List of V1Pod objects for which metrics exist.

    Raises:
        MetricsServerUnavailableError: If no pods or metrics are available for
            the deployment, or if access is forbidden.
    """
    pods = get_deployment_pods(k8s=k8s, namespace=namespace, deployment=deployment)
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
            f"Cannot access metrics for pod '{pod_name}' in namespace '{namespace}': "
            f"{e.reason} (HTTP {e.status})"
        ) from e
    except Exception as e:
        raise MetricsServerUnavailableError(
            f"Unexpected error while querying metrics for pod '{pod_name}' "
            f"in namespace '{namespace}': {str(e)}"
        ) from e

    if not metrics.get("containers"):
        raise MetricsServerUnavailableError(
            f"No metrics available for workload '{deployment.metadata.name}' "
            f"in namespace '{namespace}'. HPA scaling may not work."
        )

    return pods
