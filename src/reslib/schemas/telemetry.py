from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

from reslib.constants import EventEnum, MetricsEnum
from reslib.runtime.phases import ExecutionPhase


class EventPayload(BaseModel):
    """Base event payload that allows arbitrary additional fields."""

    model_config = ConfigDict(extra="allow")

    event_name: EventEnum = Field(..., description="Name of the event.")
    type: str = Field(default="event", description="Type of payload")
    phase: ExecutionPhase = Field(
        ..., description="Name of the phase this event belongs to."
    )
    details: Optional[str] = Field(default=None, description="Any detailed message")


class MetricsPayload(BaseModel):
    """
    Standardized payload for emitting metrics in the Resilience Library (Reslib).

    This model represents a single metrics record, which can be used for monitoring,
    observability, or alerting. It is compatible with pydantic, allowing validation
    and serialization. Additional arbitrary fields can be included beyond the
    defined schema.
    """

    model_config = ConfigDict(extra="allow")

    metrics_name: MetricsEnum = Field(..., description="Name of the metrics.")
    type: str = Field(default="metrics", description="Type of payload")
    function: str = Field(
        ..., description="Name of the function/class that's emitting this metrics."
    )
    is_error: bool = Field(default=False, description="Is error related metrics")
    details: Optional[str] = Field(default=None, description="Any additional details")
