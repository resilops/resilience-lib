import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from reslib.schemas.telemetry import EventPayload, MetricPayload


class BaseTelemetry(ABC):

    @abstractmethod
    def emit_event(self, *, event: EventPayload) -> None:
        """
        Publish an event payload.

        Args:
            event: Structured event data emitted by Reslib.
        """
        raise NotImplementedError

    @abstractmethod
    def emit_metrics(self, *, metrics: MetricPayload) -> None:
        """
        Record a metrics payload.

        Args:
            metrics: Structured metrics data emitted by Reslib.
        """
        raise NotImplementedError


class NoopTelemetry(BaseTelemetry):
    """
    No-op implementation of BaseTelemetry.

    This telemetry silently discards all events and metrics. And it is used as the
    default when no telemetry backend is configured.
    """

    def emit_event(self, *, event: EventPayload) -> None:
        return None

    def emit_metrics(self, *, metrics: MetricPayload) -> None:
        return None


@dataclass
class TimedResponse:
    response: httpx.Response
    latency: float
    timestamp: datetime  # UTC timestamp when request was sent


async def send_timed_request(client: httpx.AsyncClient, endpoint: str) -> TimedResponse:
    """
    Send an HTTP GET request, measure latency, and record request timestamp.

    Raises exceptions directly — they will be handled after requests complete.
    """
    timestamp = datetime.now(timezone.utc)
    start = time.perf_counter()
    response = await client.get(endpoint)
    latency = (time.perf_counter() - start) * 1000

    response.raise_for_status()

    return TimedResponse(response=response, latency=latency, timestamp=timestamp)


def utc_now_iso() -> str:
    """Return the current UTC time serialized as ISO 8601."""
    return datetime.now(timezone.utc).isoformat()
