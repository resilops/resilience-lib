import asyncio
import logging
import random
from typing import Dict, List

from kubernetes.client import V1DeleteOptions, V1Pod

from reslib import helpers as h
from reslib.actions.schemas import PodTerminationSchema
from reslib.core.context import get_context, set_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import PodDeletionTimeoutError, PodsSelectionError
from reslib.k8s.pods import (
    get_pod_termination_timeout,
    get_workload_pods,
    pod_exists,
)
from reslib.k8s.schema import WorkloadState
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.validators import QuantitySelection

logger = logging.getLogger(__name__)


async def build_pod_deletion_task(
    k8s: KubernetesClient, pod: V1Pod, namespace: str, timeout: int
):
    """
    Delete a pod and return a watchable task to monitor its deletion.

    This function immediately issues a deletion request for the given pod
    and returns a tuple containing a coroutine that completes when the pod
    is fully removed, along with a descriptive task name. It is intended
    for use with `watch_task_group` or similar task orchestration utilities.

    Args:
        k8s (KubernetesClient): Kubernetes client instance.
        pod (V1Pod): Pod object to delete.
        namespace (str): Namespace where the pod resides.
        timeout (int): Maximum time in seconds to wait for the pod deletion.

    Returns:
        Tuple[Awaitable, str]: A coroutine that resolves when the pod no
        longer exists, and a string task name in the format `delete:pod:<pod_name>`.

    Raises:
        PodDeletionTimeoutError: If the pod is not deleted within the specified timeout.
    """
    await k8s.delete_namespaced_pod(
        name=pod.metadata.name, namespace=namespace, body=V1DeleteOptions()
    )
    set_context("last_pod_killed_at", h.utc_now_iso())
    return (
        watch_until(
            condition=pod_exists,
            timeout=timeout,
            poll_interval=5,
            namespace=namespace,
            pod_name=pod.metadata.name,
            k8s=k8s,
            timeout_exception=PodDeletionTimeoutError(
                error_code="POD_DELETION_TIMEOUT",
                message="Pod was not deleted within the allowed timeout.",
                namespace=namespace,
                workload=get_context("scenario").template.workload,
                context={
                    "rule": "pod no longer exists before timeout",
                    "inputs": {
                        "pod_name": pod.metadata.name,
                        "namespace": namespace,
                        "timeout_seconds": timeout,
                    },
                    "observed": {
                        "deletion_requested": True,
                    },
                },
                fix_hint=(
                    "Check pod finalizers, termination grace period, "
                    "or node health preventing deletion."
                ),
                retryable=True,
            ),
        ),
        f"delete:pod:{pod.metadata.name}",
    )


async def terminate_pods(**kwargs) -> Dict:
    """
    Terminate one or more running pods from a workload and wait for their deletion.

    Steps:
      1. Validate and parse arguments using `TerminatePodsArgs`.
      2. Determine the number of pods to terminate based on ready replicas.
      3. Select running pods matching the workload name.
      4. Delete pods concurrently and wait for all to finish.

    Expected keyword arguments (`**kwargs`):
        namespace (str): Kubernetes namespace of the workload.
        workload (str): Name of the deployment/workload
        quantity (int): Number of pods to terminate.
        mode (QuantitySelectionModeEnum): Selection mode ('absolute' or 'percentage').
        event_handler (BaseEventRecorder, optional): Recorder to log metrics/events.

    Raises:
        PodsSelectionError: If no pods are selected or no running pods are found.
        PodDeletionTimeoutError: If pod deletion does not complete within the timeout.
    """
    logger.info("Starting pod termination")
    args = PodTerminationSchema(**kwargs)

    # 1. Discover workload
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace
    workload_name: str = scenario.template.workload

    # 2. Determine pods to terminate
    selection = QuantitySelection(
        mode=scenario.template.mode, amount=scenario.template.quantity
    )
    pods_to_terminate = selection.with_total(workload.runtime.ready_replicas)

    logger.info(f"Total pods to terminate is: {pods_to_terminate}")

    if pods_to_terminate <= 0:
        raise PodsSelectionError(
            error_code="NO_PODS_SELECTED_FOR_TERMINATION",
            message="Resolved pod termination count is zero; nothing to terminate.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "pods_to_terminate >= 1",
                "inputs": {
                    "mode": scenario.template.mode.value,
                    "quantity": scenario.template.quantity,
                    "ready_replicas": workload.runtime.ready_replicas,
                },
                "observed": {
                    "pods_to_terminate": pods_to_terminate,
                },
            },
            fix_hint=(
                "Increase `quantity` (or percentage) or ensure "
                "the workload has ready replicas."
            ),
            retryable=False,
        )

    # 3. List candidate pods
    k8s = KubernetesClient()

    pods = await get_workload_pods(
        k8s=k8s, namespace=namespace, workload_spec=workload.spec
    )
    candidate_pods: List[V1Pod] = random.sample(pods, k=pods_to_terminate)

    if not candidate_pods:
        raise PodsSelectionError(
            error_code="NO_CANDIDATE_PODS_FOUND",
            message="No eligible running pods were found for termination.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "at least one candidate pod must be available for termination",
                "inputs": {
                    "namespace": namespace,
                    "workload": workload_name,
                    "requested_terminations": pods_to_terminate,
                },
                "observed": {
                    "total_pods_found": len(pods),
                    "candidate_pods_selected": len(candidate_pods),
                },
            },
            fix_hint=(
                "Ensure the workload has running pods and the label selector matches. "
                "If the workload is scaling down or restarting, retry after "
                "it stabilizes."
            ),
            retryable=True,
        )

    # 4. Terminate pods concurrently
    timeout: int = get_pod_termination_timeout(
        candidate_pods, max_timeout=args.timeout_seconds
    )
    deletion_tasks = []
    for pod in candidate_pods:
        deletion_tasks.append(
            await build_pod_deletion_task(
                k8s=k8s, pod=pod, namespace=namespace, timeout=timeout
            )
        )
    await watch_task_group(
        tasks=deletion_tasks, timeout=timeout + 5, return_when=asyncio.FIRST_EXCEPTION
    )
    logger.info("Pod termination successful")
    return {
        "result": "pods_terminated",
        "reason": (
            "Selected workload pods were successfully terminated and confirmed deleted."
        ),
        "observed": {
            "requested_terminations": pods_to_terminate,
            "terminated_pods": len(candidate_pods),
            "termination_timeout_seconds": timeout,
            "last_pod_killed_at": get_context("last_pod_killed_at"),
        },
    }
