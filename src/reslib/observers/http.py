import asyncio
from typing import Optional

import httpx

from reslib import helpers as h
from reslib.constants import MetricsEnum
from reslib.core.watchdog import watch_task_group
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import get_workload
from reslib.schemas.http import HTTPLatencyArgsTemplate
from reslib.schemas.telemetry import MetricsPayload


def _emit_metrics(
    *,
    workload: WorkloadState,
    telemetry: h.BaseTelemetry,
    timed_response: Optional[h.TimedResponse] = None,
    error: Optional[Exception] = None,
) -> None:
    """Emit a single observer metric with optional error or latency info."""
    metrics = MetricsPayload(
        metrics_name=MetricsEnum.HTTP,
        function="measure_endpoint_latency",
        workload_state=workload.model_dump(),
    )

    if timed_response:
        metrics.status_code = timed_response.response.status_code
        metrics.latency = timed_response.latency
        metrics.request_timestamp = timed_response.timestamp

    if error:
        metrics.is_error = True
        metrics.details = str(error)

    telemetry.emit_metrics(metrics=metrics)


async def measure_endpoint_latency(**kwargs) -> None:
    """
    Measure HTTP latency for a workload endpoint and emit observer metrics.

    Steps:
        1. Parse and validate arguments via `MeasureHTTPLatencyArgs`.
        2. Send multiple concurrent HTTP GET requests to the endpoint.
        3. Fetch workload state *after* requests complete.
        4. Emit one event per request with latency or error information.

    Raises:
        WorkloadNotFound, MultipleWorkloadsReturned, TimeoutError, Exception
    """
    args = HTTPLatencyArgsTemplate(**kwargs)

    async with httpx.AsyncClient(timeout=args.timeout) as client:
        # 1. Build request coroutines
        tasks = [
            (
                h.send_timed_request(client=client, endpoint=args.endpoint),
                f"request:{n}",
            )
            for n in range(args.requests_per_interval)
        ]

        # 2. Execute all tasks concurrently, do not propagate exceptions except timeout
        completed_tasks = await watch_task_group(
            tasks=tasks,
            timeout=args.timeout * args.requests_per_interval,
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=False,
        )

    # 3. Fetch workload state AFTER requests finish
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)

    # 4. Emit events for each completed request
    for task in completed_tasks:
        try:
            response: h.TimedResponse = task.result()
            _emit_metrics(
                workload=workload,
                timed_response=response,
                telemetry=args.telemetry,
            )
        except Exception as exc:
            # Already handled by monitor_tasks raising, just send metrics
            _emit_metrics(workload=workload, error=exc, telemetry=args.telemetry)
            raise
