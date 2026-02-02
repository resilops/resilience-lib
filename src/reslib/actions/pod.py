import asyncio
import logging
from typing import List

from kubernetes.client import V1DeleteOptions, V1Pod

from reslib.core.watchdog import monitor_tasks
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import PodDeletionTimeoutError, PodsSelectionError
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import (
    get_deployment_pods,
    get_pod_termination_timeout,
    get_workload,
    pod_exists,
)
from reslib.schemas.pod import PodTerminationArgsTemplate
from reslib.schemas.validators import QuantitySelection

logger = logging.getLogger(__name__)


async def _delete_pod_and_wait_task(
    namespace: str, pod: V1Pod, k8s: KubernetesClient, interval: float = 4.0
) -> None:
    """
    Delete a single pod and wait until it is fully removed.

    Args:
        namespace: Kubernetes namespace.
        pod: Pod object to delete.
        k8s: Kubernetes client instance.
        interval: Seconds between polling for pod existence.

    Raises:
        PodDeletionTimeoutError: If pod is not deleted within timeout.
    """
    logger.info("Requesting deletion of pod %s/%s", namespace, pod.metadata.name)
    k8s.v1_api.delete_namespaced_pod(
        name=pod.metadata.name, namespace=namespace, body=V1DeleteOptions()
    )

    while True:
        if not pod_exists(namespace=namespace, pod_name=pod.metadata.name, k8s=k8s):
            logger.info("Pod %s/%s successfully deleted", namespace, pod.metadata.name)
            return
        await asyncio.sleep(interval)


async def terminate_pods(**kwargs) -> None:
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
            f"Pods to terminate: {pods_to_terminate}. "
            f"Either change percentage or use absolute quantity"
        )

    # 3. List candidate pods
    k8s = KubernetesClient()
    deployment = k8s.apps.read_namespaced_deployment(
        name=args.workload,
        namespace=args.namespace,
    )

    pods = get_deployment_pods(k8s=k8s, namespace=args.namespace, deployment=deployment)
    candidate_pods: List[V1Pod] = pods[:pods_to_terminate]

    if not candidate_pods:
        raise PodsSelectionError(
            f"No running pods found to terminate for {args.namespace}/{args.workload}"
        )

    # 4. Terminate pods concurrently
    try:
        await monitor_tasks(
            watch_tasks=[
                _delete_pod_and_wait_task(namespace=args.namespace, pod=pod, k8s=k8s)
                for pod in candidate_pods
            ],
            timeout=get_pod_termination_timeout(candidate_pods),
            return_when=asyncio.FIRST_EXCEPTION,
        )
    except TimeoutError:
        raise PodDeletionTimeoutError(
            f"Timed out waiting for pods to be deleted in namespace '{args.namespace}'"
        )

    logger.info("Pod termination successful")
