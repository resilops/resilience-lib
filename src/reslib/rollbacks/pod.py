import asyncio
import logging
from typing import Any, Awaitable, List, Tuple

from reslib.constants import (
    CONTAINER_CRASH_MONITOR_TASK_NAME,
    REACHED_DESIRED_REPLICA_TASK_NAME,
)
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import ReachedDesiredReplicaError
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import (
    get_workload,
    raise_on_container_fail,
    raise_on_desired_replicas,
)
from reslib.schemas.pod import PodTerminationArgsTemplate

logger = logging.getLogger(__name__)


async def wait_until_pod_respawn(**kwargs) -> dict:
    """
    Wait for a Kubernetes Deployment to recover to its desired replica count.

    The function monitors two conditions concurrently:
    1. The Deployment reaches (or exceeds) its desired number of ready replicas
       (success condition).
    2. Any container failure occurs during the wait (fail-fast condition).

    Args:
        **kwargs: Arguments used to construct PodTerminationArgsTemplate.

    Returns:
        A context dictionary describing the successful replica state.

    Raises:
        ContainerFailureError: If any container enters a failed state while waiting.
        asyncio.TimeoutError: If the Deployment does not recover within the timeout.
    """
    args = PodTerminationArgsTemplate(**kwargs)
    k8s = KubernetesClient()
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)

    tasks: List[Tuple[Awaitable[Any], str]] = [
        (
            # Success condition: desired replicas reached
            watch_until(
                condition=raise_on_desired_replicas,
                timeout=args.pod_respawn_timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                workload_name=args.workload,
                namespace=args.namespace,
            ),
            REACHED_DESIRED_REPLICA_TASK_NAME,
        ),
        (
            # Fail fast on container crash
            watch_until(
                condition=raise_on_container_fail,
                timeout=args.pod_respawn_timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                workload_spec=workload.spec,
                namespace=args.namespace,
            ),
            CONTAINER_CRASH_MONITOR_TASK_NAME,
        ),
    ]

    try:
        await watch_task_group(
            tasks=tasks,
            timeout=args.pod_respawn_timeout_seconds + 30,
            return_when=asyncio.FIRST_EXCEPTION,
        )
    except ReachedDesiredReplicaError as exc:
        # Expected exit path when scaling completes successfully
        logger.info("Pods reached desire state. Stopping pod watch")
        return exc.context
