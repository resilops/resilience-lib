import time
import httpx
import asyncio
from typing import Tuple, Any

from reslib.constants import ReslibEventEnum
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import get_single_workload
from reslib.schemas.event import EventPayload
from reslib.runtime.phases import ExecutionPhase
from reslib.observers.schema import MeasureHTTPLatencyArgs


async def _send_timed_request(
    *, client: httpx.AsyncClient, endpoint: str
) -> Tuple[httpx.Response, float]:
    """
    Send an HTTP GET request and measure the total request latency.

    Args:
        client: Shared AsyncClient instance.
        endpoint: Full HTTP endpoint URL.

    Returns:
        A tuple of (httpx.Response, latency_in_seconds).

    Raises:
        httpx.HTTPError: Propagates any request-related errors.
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
        2. Send multiple concurrent HTTP GET requests to the specified endpoint.
        3. Measure latency and capture any request errors.
        4. Fetch workload state after requests complete.
        5. Emit one event per request with latency and workload state.

    Expected keyword arguments (`**kwargs`):
        namespace: Kubernetes namespace of the workload.
        labels: Label selector identifying the workload.
        endpoint: Full HTTP URL to probe (e.g., http://service/health).
        timeout: Per-request timeout in seconds.
        requests_per_interval: Number of parallel requests to send.
        event_recorder: Recorder used to emit observer metrics.

    Raises:
        WorkloadNotFound: If no workloads match the selector.
        MultipleWorkloadsReturned: If more than one workload matches.
    """
    args = MeasureHTTPLatencyArgs(**kwargs)

    # 1. Send requests concurrently
    async with httpx.AsyncClient(timeout=args.timeout) as client:
        tasks = [
            _send_timed_request(client=client, endpoint=args.endpoint)
            for _ in range(args.requests_per_interval)
        ]
        responses: Tuple[BaseException | Any] = await asyncio.gather(
            *tasks, return_exceptions=True
        )

    # 2. Fetch workload state *after* requests complete
    workload: WorkloadState = get_single_workload(
        namespace=args.namespace, labels=args.labels
    )

    # 3. Emit observer events
    for response in responses:
        event = EventPayload(
            event_name=ReslibEventEnum.OBSERVER_METRICS,
            phase=ExecutionPhase.OBSERVER,
            workload_state=workload.dict(),
            observer_name="http_latency_with_state"
        )

        if isinstance(response, Exception):
            event.is_error = True
            event.error_msg = str(response)
        else:
            response_obj, latency = response
            event.status_code = response_obj.status_code
            event.latency = latency

        args.event_recorder.record(event=event)
