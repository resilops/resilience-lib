from abc import ABC, abstractmethod

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
