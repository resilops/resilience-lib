import asyncio
import logging
from typing import Any, Awaitable, List, Tuple

from reslib.core.context import get_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import (
    RollingRestartCompleteError,
    RollingRestartTimeoutError,
)
from reslib.k8s.pods import raise_on_container_fail
from reslib.k8s.schema import WorkloadState
from reslib.k8s.workloads import raise_on_rolling_restart_complete
from reslib.rollbacks.schemas import RollingRestartTimeout
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)


async def wait_until_rolling_restart_complete(**kwargs):
    """
    Wait for a rolling restart to complete and fail fast on pod/container errors.
    """
    args = RollingRestartTimeout(**kwargs)
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace
    started_at = get_context("rolling_restart_started_at")
    target_generation = get_context(
        "rolling_restart_generation",
        default=None,
        raise_error=False,
    )
    k8s = KubernetesClient()

    tasks: List[Tuple[Awaitable[Any], str]] = [
        (
            watch_until(
                condition=raise_on_rolling_restart_complete,
                timeout=args.timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                workload_name=workload.spec.name,
                namespace=namespace,
                started_at=started_at,
                target_generation=target_generation,
                timeout_exception=RollingRestartTimeoutError(
                    error_code="ROLLING_RESTART_TIMEOUT",
                    message=(
                        f"Deployment '{workload.spec.name}' did not complete "
                        f"rolling restart within {args.timeout_seconds} seconds."
                    ),
                    fix_hint=(
                        "Inspect rollout status, pod events, readiness probes, "
                        "image pull status, config, secrets, and dependencies."
                    ),
                ),
            ),
            "monitor:rolling-restart:complete",
        ),
        (
            watch_until(
                condition=raise_on_container_fail,
                timeout=args.timeout_seconds,
                poll_interval=5,
                k8s=k8s,
                workload_spec=workload.spec,
                namespace=namespace,
            ),
            "monitor:rolling-restart:container-errors",
        ),
    ]

    try:
        await watch_task_group(
            tasks=tasks,
            timeout=args.timeout_seconds + 30,
            return_when=asyncio.FIRST_EXCEPTION,
        )
    except RollingRestartCompleteError:
        logger.info("Rolling restart completed")
        return {
            "result": "rolling_restart_completed",
            "status": "success",
            "reason": "Deployment completed rolling restart successfully.",
            "observed": get_context("rolling_restart_complete"),
        }
