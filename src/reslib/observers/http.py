import asyncio
from bisect import bisect_right
from collections import Counter, deque
from typing import Deque, Sequence

import httpx

from reslib import helpers as h
from reslib.constants import MetricsEnum
from reslib.core.context import get_context
from reslib.core.watchdog import watch_task_group
from reslib.k8s.client import KubernetesClient
from reslib.k8s.schema import WorkloadRuntimeState
from reslib.k8s.workloads import get_workload_runtime
from reslib.observers.schemas import HTTPLatencyArgsTemplate
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.telemetry import MetricPayload

# Explicit cumulative latency bucket bounds in milliseconds.
# These are used to build mergeable interval histograms.
LATENCY_BUCKET_BOUNDS_MS = [
    5,
    10,
    15,
    20,
    25,
    30,
    40,
    50,
    75,
    100,
    150,
    200,
    300,
    500,
    750,
    1000,
    2000,
    5000,
]

ERROR_SAMPLE_LIMIT = 20
HTTP_ERROR_STATUS_MIN = 500


def _build_latency_buckets(latencies: list[float]) -> dict[str, int]:
    """
    Build cumulative latency buckets from raw latency samples.

    Example:
        latencies = [7, 18, 42]
        bounds = [5, 10, 20, 50]

        result = {
            "le_5": 0,
            "le_10": 1,
            "le_20": 2,
            "le_50": 3,
            "gt_50": 0,
        }
    """
    buckets: dict[str, int] = {}

    if not latencies:
        for bound in LATENCY_BUCKET_BOUNDS_MS:
            buckets[f"le_{bound}"] = 0
        buckets[f"gt_{LATENCY_BUCKET_BOUNDS_MS[-1]}"] = 0
        return buckets

    sorted_latencies = sorted(latencies)

    for bound in LATENCY_BUCKET_BOUNDS_MS:
        buckets[f"le_{bound}"] = bisect_right(sorted_latencies, bound)

    last_bound = LATENCY_BUCKET_BOUNDS_MS[-1]
    buckets[f"gt_{last_bound}"] = len(sorted_latencies) - buckets[f"le_{last_bound}"]

    return buckets


def _build_measurement(
    *,
    timed_responses: list[h.TimedResponse],
    transport_error_count: int,
    interval_start: str,
    interval_end: str,
) -> dict[str, object]:
    """
    Build the aggregated interval measurement payload.

    This payload is designed to be mergeable across many observer intervals.
    """
    status_code_counts = Counter(
        str(response.response.status_code) for response in timed_responses
    )
    latencies = [response.latency for response in timed_responses]

    http_error_count = sum(
        1
        for response in timed_responses
        if response.response.status_code >= HTTP_ERROR_STATUS_MIN
    )

    request_count = len(timed_responses) + transport_error_count
    success_count = len(timed_responses)
    error_count_total = transport_error_count + http_error_count

    measurement: dict[str, object] = {
        "aggregation_temporality": "delta",
        "request_count": request_count,
        "success_count": success_count,
        "transport_error_count": transport_error_count,
        "http_error_count": http_error_count,
        "error_count_total": error_count_total,
        "status_code_counts": dict(status_code_counts),
        "latency_histogram_type": "cumulative",
        "latency_bucket_bounds_ms": LATENCY_BUCKET_BOUNDS_MS,
        "interval_start": interval_start,
        "interval_end": interval_end,
    }

    if latencies:
        measurement.update(
            {
                "latency_ms_sum": sum(latencies),
                "latency_ms_avg": sum(latencies) / len(latencies),
                "latency_ms_min": min(latencies),
                "latency_ms_max": max(latencies),
                "latency_buckets_ms": _build_latency_buckets(latencies),
            }
        )
    else:
        measurement.update(
            {
                "latency_ms_sum": 0.0,
                "latency_buckets_ms": _build_latency_buckets([]),
            }
        )

    return measurement


def _build_http_metric_payload(
    *,
    state: WorkloadRuntimeState,
    timed_responses: list[h.TimedResponse],
    error_samples: Sequence[Exception],
    transport_error_count: int,
    interval_start: str,
    interval_end: str,
) -> MetricPayload:
    """Build the aggregated HTTP metric payload for one observer interval."""
    measurement = _build_measurement(
        timed_responses=timed_responses,
        transport_error_count=transport_error_count,
        interval_start=interval_start,
        interval_end=interval_end,
    )

    return MetricPayload(
        metrics_name=MetricsEnum.HTTP,
        function="measure_endpoint_latency",
        workload_state=state,
        measurement=measurement,
        is_error=bool(error_samples),
        error=error_samples[0].__class__.__name__ if error_samples else None,
        data=(
            {"errors": [str(error) for error in error_samples]}
            if error_samples
            else None
        ),
    )


def _emit_aggregated_metrics(
    *,
    state: WorkloadRuntimeState,
    timed_responses: list[h.TimedResponse],
    error_samples: Sequence[Exception],
    transport_error_count: int,
    interval_start: str,
    interval_end: str,
) -> None:
    """Emit one aggregated HTTP metric for a single observer interval."""
    telemetry: h.BaseTelemetry = get_context("telemetry")
    metrics = _build_http_metric_payload(
        state=state,
        timed_responses=timed_responses,
        error_samples=error_samples,
        transport_error_count=transport_error_count,
        interval_start=interval_start,
        interval_end=interval_end,
    )
    telemetry.emit_metrics(metrics=metrics)


def _get_request_group_timeout(
    *, request_timeout_seconds: int, requests_per_interval: int
) -> int:
    """Return the task-group timeout used for the request fan-out."""
    return max(request_timeout_seconds, request_timeout_seconds * requests_per_interval)


async def _collect_interval_requests(
    *, endpoint: str, request_timeout_seconds: int, requests_per_interval: int
) -> list[asyncio.Task]:
    """Execute one interval of concurrent HTTP requests."""
    async with httpx.AsyncClient(timeout=request_timeout_seconds) as client:
        return await watch_task_group(
            tasks=[
                (
                    h.send_timed_request(client=client, endpoint=endpoint),
                    f"request:{index}",
                )
                for index in range(requests_per_interval)
            ],
            timeout=_get_request_group_timeout(
                request_timeout_seconds=request_timeout_seconds,
                requests_per_interval=requests_per_interval,
            ),
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=False,
        )


def _split_request_results(
    completed_tasks: Sequence[asyncio.Task],
) -> tuple[list[h.TimedResponse], Deque[Exception], int]:
    """Split completed request tasks into successful responses and errors."""
    timed_responses: list[h.TimedResponse] = []
    error_samples: Deque[Exception] = deque(maxlen=ERROR_SAMPLE_LIMIT)
    transport_error_count = 0

    for task in completed_tasks:
        try:
            timed_responses.append(task.result())
        except Exception as exc:
            transport_error_count += 1
            error_samples.append(exc)

    return timed_responses, error_samples, transport_error_count


async def _get_workload_state(
    *, k8s: KubernetesClient, scenario: ResiliencyScenario
) -> WorkloadRuntimeState:
    """Fetch the latest workload runtime state for metric emission."""
    deployment = await k8s.read_namespaced_deployment(
        name=scenario.template.workload,
        namespace=scenario.template.namespace,
    )
    return get_workload_runtime(deployment=deployment, is_full=False)


async def measure_endpoint_latency(**kwargs) -> None:
    """
    Measure HTTP latency for a workload endpoint and emit one aggregated metric
    for a single observer interval.

    Emission model:
        - One metric payload per observer interval
        - Payload contains only aggregate measurements
        - Payload is designed to be merged later by a reducer task

    Steps:
        1. Parse observer arguments.
        2. Send concurrent HTTP GET requests to the endpoint.
        3. Fetch workload state after requests complete.
        4. Emit one aggregated metric for the interval.
    """
    scenario: ResiliencyScenario = get_context("scenario")
    args = HTTPLatencyArgsTemplate(**kwargs)
    k8s = KubernetesClient()
    interval_start = h.utc_now_iso()

    completed_tasks = await _collect_interval_requests(
        endpoint=args.endpoint,
        request_timeout_seconds=args.request_timeout_seconds,
        requests_per_interval=args.requests_per_interval,
    )
    interval_end = h.utc_now_iso()

    state = await _get_workload_state(k8s=k8s, scenario=scenario)
    timed_responses, error_samples, transport_error_count = _split_request_results(
        completed_tasks
    )

    _emit_aggregated_metrics(
        state=state,
        timed_responses=timed_responses,
        error_samples=error_samples,
        transport_error_count=transport_error_count,
        interval_start=interval_start,
        interval_end=interval_end,
    )
