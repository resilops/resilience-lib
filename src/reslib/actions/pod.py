import asyncio
import logging
import random
from typing import Dict, List

from kubernetes.client import V1DeleteOptions, V1Pod

from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import PodDeletionTimeoutError, PodsSelectionError
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import (
    get_pod_termination_timeout,
    get_workload,
    get_workload_pods,
    pod_exists,
)
from reslib.schemas.pod import PodTerminationArgsTemplate
from reslib.schemas.validators import QuantitySelection

logger = logging.getLogger(__name__)


def watch_pod_deletion(k8s: KubernetesClient, pod: V1Pod, namespace: str, timeout: int):
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
    k8s.v1_api.delete_namespaced_pod(
        name=pod.metadata.name, namespace=namespace, body=V1DeleteOptions()
    )
    return (
        watch_until(
            condition=pod_exists,
            timeout=timeout,
            poll_interval=5,
            namespace=namespace,
            pod_name=pod.metadata.name,
            k8s=k8s,
            timeout_exception=PodDeletionTimeoutError(
                "Timed out waiting for pods to be deleted in "
                f"namespace '{namespace}'"
            ),
        ),
        f"delete:pod:{pod.metadata.name}",
    )


async def terminate_pods(**kwargs) -> Dict[str, int]:
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
    # Validate and normalize input arguments
    args = PodTerminationArgsTemplate(**kwargs)

    # 1. Discover workload
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)

    # 2. Determine pods to terminate
    selection = QuantitySelection(mode=args.mode, amount=args.quantity)
    pods_to_terminate = selection.with_total(workload.status.ready_replicas)

    logger.info(f"Total pods to terminate is: {pods_to_terminate}")

    if pods_to_terminate <= 0:
        raise PodsSelectionError(
            "No pods selected for termination", context={"pods": pods_to_terminate}
        )

    # 3. List candidate pods
    k8s = KubernetesClient()

    pods = get_workload_pods(
        k8s=k8s, namespace=args.namespace, workload_spec=workload.spec
    )
    candidate_pods: List[V1Pod] = random.sample(pods, k=pods_to_terminate)

    if not candidate_pods:
        raise PodsSelectionError(
            "No running pods found to terminate", context={"pods": len(candidate_pods)}
        )

    # 4. Terminate pods concurrently
    timeout: int = get_pod_termination_timeout(
        candidate_pods, max_timeout=args.termination_timeout_seconds
    )
    tasks = [
        watch_pod_deletion(k8s=k8s, pod=pod, namespace=args.namespace, timeout=timeout)
        for pod in candidate_pods
    ]
    await watch_task_group(
        tasks=tasks, timeout=timeout + 5, return_when=asyncio.FIRST_EXCEPTION
    )
    logger.info("Pod termination successful")
    return {"terminated_pods": len(candidate_pods)}
