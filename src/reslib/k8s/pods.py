from datetime import datetime
from typing import Dict, List, Optional

from kubernetes.client import V1Pod
from kubernetes.client.exceptions import ApiException

from reslib.config import config
from reslib.constants import (
    POD_RUNNING_STATUS,
    POD_TERMINATED_REASONS_OK,
    POD_WAITING_REASONS_OK,
)
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import ContainerCrashedError
from reslib.k8s.schema import WorkloadSpec


def get_latest_pod_ready_time(pods: List[V1Pod]) -> Optional[datetime]:
    """Return the most recent Ready condition transition time across pods."""
    ready_times: List[datetime] = []
    for pod in pods:
        for condition in pod.status.conditions or []:
            if (
                condition.type == "Ready"
                and condition.status == "True"
                and getattr(condition, "last_transition_time", None) is not None
            ):
                ready_times.append(condition.last_transition_time)

    return max(ready_times) if ready_times else None


async def get_pods_by_labels(
    *,
    k8s: KubernetesClient,
    namespace: str,
    labels: Optional[Dict[str, str]],
    pod_phase: Optional[str] = POD_RUNNING_STATUS,
) -> List[V1Pod]:
    """Return pods matching labels and an optional phase filter."""
    selector = ",".join(f"{key}={value}" for key, value in (labels or {}).items())
    pods = await k8s.list_namespaced_pod(namespace=namespace, label_selector=selector)

    if pod_phase is None:
        return list(pods.items)
    return [pod for pod in pods.items if pod.status.phase == pod_phase]


async def get_workload_pods(
    *,
    k8s: KubernetesClient,
    namespace: str,
    workload_spec: WorkloadSpec,
    pod_phase: Optional[str] = POD_RUNNING_STATUS,
) -> List[V1Pod]:
    """Return workload pods filtered by phase."""
    return await get_pods_by_labels(
        k8s=k8s,
        namespace=namespace,
        labels=workload_spec.labels,
        pod_phase=pod_phase,
    )


async def pod_exists(
    *,
    namespace: str,
    pod_name: str,
    k8s: Optional[KubernetesClient] = None,
) -> bool:
    """Return whether a pod exists in the given namespace."""
    k8s = k8s or KubernetesClient()
    try:
        await k8s.read_namespaced_pod(name=pod_name, namespace=namespace)
        return True
    except ApiException as exc:
        if exc.status == 404:
            return False
        raise


def get_pod_termination_timeout(
    pods: List[V1Pod],
    buffer_seconds: int = 10,
    default_grace_period: int = config.pod_termination_default_grace_period,
    max_timeout: int = 300,
) -> int:
    """Compute a safe timeout for pod termination."""
    if not pods:
        raise ValueError("No pods given to calculate timeout")

    max_grace = max(
        pod.spec.termination_grace_period_seconds or default_grace_period
        for pod in pods
    )
    return min(max_grace + buffer_seconds, max_timeout)


async def raise_on_container_fail(
    k8s: KubernetesClient,
    workload_spec: WorkloadSpec,
    namespace: str,
) -> None:
    """Raise when any container enters an unexpected waiting or terminated state."""
    pods = await get_workload_pods(
        k8s=k8s,
        workload_spec=workload_spec,
        namespace=namespace,
        pod_phase=None,
    )

    for pod in pods:
        pod_name = pod.metadata.name
        for container_status in pod.status.container_statuses or []:
            container_name = container_status.name
            state = container_status.state

            if state.waiting and state.waiting.reason not in POD_WAITING_REASONS_OK:
                raise ContainerCrashedError(
                    error_code="CONTAINER_WAITING_UNEXPECTED",
                    message=(
                        f"Container '{container_name}' in pod '{pod_name}' is stuck "
                        f"in waiting state '{state.waiting.reason}'."
                    ),
                    fix_hint=(
                        "Inspect pod events and container logs, then resolve the "
                        "waiting condition before retrying."
                    ),
                )

            if (
                state.terminated
                and state.terminated.reason not in POD_TERMINATED_REASONS_OK
            ):
                raise ContainerCrashedError(
                    error_code="CONTAINER_TERMINATED_UNEXPECTED",
                    message=(
                        f"Container '{container_name}' in pod '{pod_name}' "
                        f"terminated with reason '{state.terminated.reason}' and "
                        f"exit code {state.terminated.exit_code}."
                    ),
                    fix_hint=(
                        "Check container logs and recent deployment changes, then "
                        "restart or roll back the workload if the crash persists."
                    ),
                )
