from abc import ABC, abstractmethod
from reslib.schemas.event import EventPayload


class BaseEventRecorder(ABC):

    @abstractmethod
    def record(self, *, event: EventPayload) -> None:
        """
        All the vents from this lib will be sent here. Inherit this
        to adapt to your own recorder
        """


class NoopEventRecorder(BaseEventRecorder):

    def record(self, *, event: EventPayload) -> None:
        """Default no-op event recorder for resilience scenarios."""
