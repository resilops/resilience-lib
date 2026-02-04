import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from reslib.schemas.telemetry import EventPayload, MetricsPayload


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
    def emit_metrics(self, *, metrics: MetricsPayload) -> None:
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

    def emit_metrics(self, *, metrics: MetricsPayload) -> None:
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
    timestamp = datetime.now(timezone.utc)  # capture exact send time
    start = time.perf_counter()
    response = await client.get(endpoint)
    latency = time.perf_counter() - start

    return TimedResponse(response=response, latency=latency, timestamp=timestamp)


def convert_to_bytes(mem: str) -> int:
    """
    Convert a Kubernetes memory string (e.g., '512Mi', '2Gi') to bytes.

    Args:
        mem: Memory string with optional unit (Ki, Mi, Gi, Ti).

    Returns:
        Memory in bytes as an integer.

    Raises:
        ValueError: If the input format is invalid or unknown unit.
    """
    units = {
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "Pi": 1024**5,
        "Ei": 1024**6,
    }

    # Handle numeric string with no unit
    if mem.isdigit():
        return int(mem)

    # Separate numeric value and unit
    value, unit = int(mem[:-2]), mem[-2:]
    multiplier = units.get(unit)

    if multiplier is None:
        raise ValueError(f"Unknown memory unit: {unit}")

    return value * multiplier
