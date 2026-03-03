import asyncio
import logging
from typing import Any, Awaitable, List, Tuple

from reslib.constants import (
    CONTAINER_CRASH_MONITOR_TASK_NAME,
    REACHED_DESIRED_REPLICA_TASK_NAME,
)
from reslib.core.context import get_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import ReachedDesiredReplicaError
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import (
    raise_on_container_fail,
    raise_on_desired_replicas,
)
from reslib.rollbacks.schemas import PodRespawnTimeout
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)


async def wait_until_pod_respawn(**kwargs):
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
    args = PodRespawnTimeout(**kwargs)

    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace
    k8s = KubernetesClient()

    tasks: List[Tuple[Awaitable[Any], str]] = [
        (
            # Success condition: desired replicas reached
            watch_until(
                condition=raise_on_desired_replicas,
                timeout=args.timeout_seconds,
                poll_interval=3,
                k8s=k8s,
                workload_name=workload,
                namespace=namespace,
            ),
            REACHED_DESIRED_REPLICA_TASK_NAME,
        ),
        (
            # Fail fast on container crash
            watch_until(
                condition=raise_on_container_fail,
                timeout=args.timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                workload_spec=workload.spec,
                namespace=namespace,
            ),
            CONTAINER_CRASH_MONITOR_TASK_NAME,
        ),
    ]

    try:
        await watch_task_group(
            tasks=tasks,
            timeout=args.timeout_seconds + 30,
            return_when=asyncio.FIRST_EXCEPTION,
        )
    except ReachedDesiredReplicaError as exc:
        # Expected exit path when scaling completes successfully
        logger.info("Pods reached desire state. Stopping pod watch")
        return {
            "result": "pods_respawned",
            "status": "success",
            "reason": (
                "Deployment reached the desired number of ready "
                "replicas after pod termination."
            ),
            "observed": exc.context.get("observed", {}),
        }
