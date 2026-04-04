import asyncio
from collections import Counter

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

# Cumulative latency buckets in milliseconds.
# Example:
#   le_5 = count of requests with latency <= 5ms
#   le_10 = count of requests with latency <= 10ms
LATENCY_BUCKET_BOUNDS_MS = [5, 10, 25, 50, 100, 250, 500, 1000]


def _build_latency_buckets(latencies: list[float]) -> dict[str, int]:
    """Build cumulative latency buckets from raw latency samples."""
    buckets: dict[str, int] = {}

    for bound in LATENCY_BUCKET_BOUNDS_MS:
        buckets[f"le_{bound}"] = sum(1 for latency in latencies if latency <= bound)

    buckets["gt_1000"] = sum(
        1 for latency in latencies if latency > LATENCY_BUCKET_BOUNDS_MS[-1]
    )
    return buckets


def _emit_aggregated_metrics(
    *,
    state: WorkloadRuntimeState,
    timed_responses: list[h.TimedResponse],
    errors: list[Exception],
) -> None:
    """Emit one aggregated HTTP metric for a single observer interval."""

    telemetry: h.BaseTelemetry = get_context("telemetry")
    scenario: ResiliencyScenario = get_context("scenario")

    request_count = len(timed_responses) + len(errors)
    success_count = len(timed_responses)
    error_count = len(errors)

    status_code_counts = Counter(
        str(response.response.status_code) for response in timed_responses
    )
    latencies = [response.latency for response in timed_responses]

    measurement: dict[str, object] = {
        "request_count": request_count,
        "success_count": success_count,
        "error_count": error_count,
        "status_code_counts": dict(status_code_counts),
    }

    if timed_responses:
        timestamps = [response.timestamp for response in timed_responses]
        measurement.update(
            {
                "latency_ms_sum": sum(latencies),
                "latency_ms_min": min(latencies),
                "latency_ms_max": max(latencies),
                "latency_buckets_ms": _build_latency_buckets(latencies),
                "interval_start": min(timestamps),
                "interval_end": max(timestamps),
            }
        )

    metrics = MetricsPayload(
        metrics_name=MetricsEnum.HTTP,
        namespace=scenario.template.namespace,
        workload=scenario.template.workload,
        function="measure_endpoint_latency",
        workload_state=state,
        measurement=measurement,
        is_error=bool(errors),
        error=errors[0].__class__.__name__ if errors else None,
        data={"errors": [str(error) for error in errors]} if errors else None,
    )

    telemetry.emit_metrics(metrics=metrics)


async def measure_endpoint_latency(**kwargs) -> None:
    """
    Measure HTTP latency for a workload endpoint and emit one aggregated metric
    for the full request interval.

    Steps:
        1. Parse observer arguments.
        2. Send concurrent HTTP GET requests to the endpoint.
        3. Fetch workload state after requests complete.
        4. Emit one aggregated metric for the interval.
        5. Re-raise the first error, if any request failed.
    """
    scenario: ResiliencyScenario = get_context("scenario")
    args = HTTPLatencyArgsTemplate(**kwargs)
    k8s = KubernetesClient()

    async with httpx.AsyncClient(timeout=args.request_timeout_seconds) as client:
        tasks = [
            (
                h.send_timed_request(client=client, endpoint=args.endpoint),
                f"request:{n}",
            )
            for n in range(args.requests_per_interval)
        ]

        completed_tasks = await watch_task_group(
            tasks=tasks,
            timeout=max(
                args.request_timeout_seconds,
                args.request_timeout_seconds * args.requests_per_interval,
            ),
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=False,
        )

    deployment = k8s.apps.read_namespaced_deployment(
        name=scenario.template.workload,
        namespace=scenario.template.namespace,
    )
    state: WorkloadRuntimeState = get_workload_runtime(
        deployment=deployment,
        is_full=False,
    )

    timed_responses: list[h.TimedResponse] = []
    errors: list[Exception] = []

    for task in completed_tasks:
        try:
            timed_responses.append(task.result())
        except Exception as exc:
            errors.append(exc)

    _emit_aggregated_metrics(
        state=state,
        timed_responses=timed_responses,
        errors=errors,
    )

    if errors:
        raise errors[0]
