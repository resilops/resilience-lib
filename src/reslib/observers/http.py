import asyncio
import time
from typing import Optional, Tuple

import httpx

from reslib import helpers as h
from reslib.constants import MetricsEnum
from reslib.core.watchdog import monitor_tasks
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import get_workload
from reslib.observers.schema import MeasureHTTPLatencyArgs
from reslib.schemas.telemetry import MetricsPayload


def _emit_metrics(
    *,
    workload: WorkloadState,
    telemetry: h.BaseTelemetry,
    response: Optional[httpx.Response] = None,
    latency: Optional[float] = None,
    error: Optional[Exception] = None,
):
    """Emit a single observer metric with optional error or latency info."""
    metrics = MetricsPayload(
        metrics_name=MetricsEnum.HTTP,
        function="measure_http_latency",
        workload_state=workload.model_dump(),
    )

    if response:
        metrics.status_code = response.status_code
        metrics.latency = latency

    if error:
        metrics.is_error = True
        metrics.details = str(error)

    telemetry.emit_metrics(metrics=metrics)


async def _send_timed_request(
    client: httpx.AsyncClient, endpoint: str
) -> Tuple[httpx.Response, float]:
    """
    Send an HTTP GET request and measure latency.

    Raises exceptions directly — they will be handled after requests complete.
    """
    start = time.perf_counter()
    response = await client.get(endpoint)
    latency = time.perf_counter() - start
    return response, latency


async def measure_http_latency(**kwargs) -> None:
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
    args = MeasureHTTPLatencyArgs(**kwargs)

    async with httpx.AsyncClient(timeout=args.timeout) as client:
        # 1. Build request coroutines
        watch_tasks = [
            _send_timed_request(client=client, endpoint=args.endpoint)
            for _ in range(args.requests_per_interval)
        ]

        # 2. Execute all tasks concurrently, do not propagate exceptions except timeout
        completed_tasks = await monitor_tasks(
            watch_tasks=watch_tasks,
            timeout=args.timeout * args.requests_per_interval,
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=False,
        )

    # 3. Fetch workload state AFTER requests finish
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)

    # 4. Emit events for each completed request
    for task in completed_tasks:
        try:
            response, latency = task.result()
            _emit_metrics(
                workload=workload,
                response=response,
                latency=latency,
                telemetry=args.telemetry,
            )
        except Exception as exc:
            # Already handled by monitor_tasks raising, just send metrics
            _emit_metrics(workload=workload, error=exc, telemetry=args.telemetry)
            raise
