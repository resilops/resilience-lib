import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, List, Tuple

from reslib.constants import (
    CONTAINER_CRASH_MONITOR_TASK_NAME,
    REPLICAS_RESTORED_TASK_NAME,
)
from reslib.core.context import get_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import ReplicasRestoredError
from reslib.k8s.pods import raise_on_container_fail
from reslib.k8s.scaling import (
    HPA_SCALE_DOWN_EVENT_CONTEXT_KEY,
    raise_on_replicas_restored,
    wait_for_hpa_scale_down_event,
)
from reslib.k8s.schema import WorkloadState
from reslib.rollbacks.schemas import HpaScaleDownSchema
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)


async def wait_for_hpa_scale_down(**kwargs):
    """
    Wait for a workload to stabilize after HPA-induced scaling.

    This function monitors two aspects concurrently:

      1. **Container health** — using `raise_on_container_fail` to detect any
         container crashes or abnormal pod states during rollback.
      2. **Replica restoration** — using `raise_on_replicas_restored` to detect
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
                - timeout_seconds (int): Maximum wait for HPA downscale

    Returns:
        Dict[str, Any]: Context returned by `raise_on_replicas_restored` when
                        replicas reach the initial count.

    Raises:
        ReplicasRestoredError: Internally caught and returned as positive signal.
        Any other exception (container crash, timeout, API error) will propagate
        to the calling execution phase.
    """
    args = HpaScaleDownSchema(**kwargs)
    k8s = KubernetesClient()
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace
    stress_context = get_context("stress_context")
    peak_replicas_on_stress = stress_context.get("ready_replicas")
    rollback_start_time = datetime.now(timezone.utc)

    scale_down_event_task = asyncio.create_task(
        wait_for_hpa_scale_down_event(
            k8s=k8s,
            namespace=namespace,
            workload=workload,
            peak_replicas=peak_replicas_on_stress,
            not_before=rollback_start_time,
        )
    )

    rollback_tasks: List[Tuple[Awaitable[Any], str]] = [
        (
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
        (
            watch_until(
                condition=raise_on_replicas_restored,
                timeout=args.timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                namespace=namespace,
                stress_context=stress_context,
            ),
            REPLICAS_RESTORED_TASK_NAME,
        ),
    ]

    try:
        await watch_task_group(
            tasks=rollback_tasks,
            timeout=args.timeout_seconds + 10,
            return_when=asyncio.FIRST_EXCEPTION,
        )
    except ReplicasRestoredError as exc:
        logger.info("Replicas restored. Rollback success")
        observed = exc.context.get("observed", {})
        scale_down_event = (
            get_context(
                HPA_SCALE_DOWN_EVENT_CONTEXT_KEY,
                raise_error=False,
            )
            or {}
        )
        return {
            "result": "hpa_scale_down_stabilized",
            "status": "success",
            "reason": (
                "Workload replicas and CPU utilization stabilized after "
                "HPA scale-up caused by CPU stress."
            ),
            "observed": {**observed, **scale_down_event},
        }
    finally:
        scale_down_event_task.cancel()
