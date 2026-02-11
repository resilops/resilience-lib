import asyncio
import logging
from typing import Any, Awaitable, List, Tuple

from reslib.constants import (
    CONTAINER_CRASH_MONITOR_TASK_NAME,
    REPLICAS_RESTORED_TASK_NAME,
)
from reslib.core.context import get_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import ReplicasRestoredError
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import (
    get_workload,
    raise_on_container_fail,
    raise_on_replicas_restored_cpu,
)
from reslib.schemas.hpa import HpaCPUStressArgsTemplate

logger = logging.getLogger(__name__)


async def wait_until_hpa_scales_down(**kwargs):
    """
    Wait for a workload to stabilize after HPA-induced scaling.

    This function monitors two aspects concurrently:

      1. **Container health** — using `raise_on_container_fail` to detect any
         container crashes or abnormal pod states during rollback.
      2. **Replica restoration** — using `raise_on_replicas_restored_cpu` to detect
         when the Deployment replicas have returned to the initial state
         captured before the experiment.

    The function returns **only positive results** (replicas restored).
    All other exceptions (e.g., container failures, timeouts) are propagated
    to the caller (typically handled by the phase execution framework).

    Execution is performed concurrently with polling intervals and a global timeout.

    Args:
        **kwargs: Keyword arguments matching template, which must include:
                - workload (str): Deployment name
                - namespace (str): Kubernetes namespace
                - hpa_scale_down_timeout_seconds (int): Maximum wait for HPA downscale

    Returns:
        Dict[str, Any]: Context returned by `raise_on_replicas_restored_cpu` when
                        replicas reach the initial count.

    Raises:
        ReplicasRestoredError: Internally caught and returned as positive signal.
        Any other exception (container crash, timeout, API error) will propagate
        to the calling execution phase.
    """

    args = HpaCPUStressArgsTemplate(**kwargs)
    k8s = KubernetesClient()
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)

    tasks: List[Tuple[Awaitable[Any], str]] = [
        (
            watch_until(
                condition=raise_on_container_fail,
                timeout=args.hpa_scale_down_timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                workload_spec=workload.spec,
                namespace=args.namespace,
            ),
            CONTAINER_CRASH_MONITOR_TASK_NAME,
        ),
        (
            watch_until(
                condition=raise_on_replicas_restored_cpu,
                timeout=args.hpa_scale_down_timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                namespace=args.namespace,
                stress_context=get_context("stress_context"),
            ),
            REPLICAS_RESTORED_TASK_NAME,
        ),
    ]

    try:
        await watch_task_group(
            tasks=tasks,
            timeout=args.hpa_scale_down_timeout_seconds + 10,
            return_when=asyncio.FIRST_EXCEPTION,
        )
    except ReplicasRestoredError as exc:
        logger.info("Replicas restored. Rollback success")
        return exc.context
