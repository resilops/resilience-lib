import asyncio
from typing import Optional

import httpx

from reslib import helpers as h
from reslib.constants import MetricsEnum
from reslib.core.context import get_context
from reslib.core.watchdog import watch_task_group
from reslib.k8s.client import KubernetesClient
from reslib.k8s.schema import WorkloadRuntimeState
from reslib.k8s.utils import get_workload_runtime
from reslib.observers.schemas import HTTPLatencyArgsTemplate
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.telemetry import MetricsPayload


def _emit_metrics(
    *,
    status: WorkloadRuntimeState,
    timed_response: Optional[h.TimedResponse] = None,
    error: Optional[Exception] = None,
) -> None:
    """Emit a single observer metric with optional error or latency info."""

    telemetry: h.BaseTelemetry = get_context("telemetry")
    scenario: ResiliencyScenario = get_context("scenario")

    metrics = MetricsPayload(
        metrics_name=MetricsEnum.HTTP,
        namespace=scenario.template.namespace,
        workload=scenario.template.workload,
        function="measure_endpoint_latency",
        workload_status=status,
    )

    if timed_response:
        metrics.measurement = {
            "status_code": timed_response.response.status_code,
            "latency_ms": timed_response.latency,
            "timestamp": timed_response.timestamp,
        }

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
    scenario: ResiliencyScenario = get_context("scenario")
    args = HTTPLatencyArgsTemplate(**kwargs)
    k8s = KubernetesClient()

    async with httpx.AsyncClient(timeout=args.request_timeout_seconds) as client:
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
            timeout=args.request_timeout_seconds * args.requests_per_interval,
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=False,
        )

    # Get deployment
    deployment = k8s.apps.read_namespaced_deployment(
        name=scenario.template.workload,
        namespace=scenario.template.namespace,
    )

    # 3. Fetch workload status AFTER requests finish
    status: WorkloadRuntimeState = get_workload_runtime(deployment=deployment)

    # 4. Emit events for each completed request
    for task in completed_tasks:
        try:
            response: h.TimedResponse = task.result()
            _emit_metrics(status=status, timed_response=response)
        except Exception as exc:
            # Already handled by monitor_tasks raising, just send metrics
            _emit_metrics(status=status, error=exc)
            raise
