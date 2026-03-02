from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from reslib.constants import EventEnum, MetricsEnum
from reslib.runtime.phases import ExecutionPhase


class HTTPLatencyArgsTemplate(BaseModel):
    """
    Arguments for the `measure_http_latency` function.

    Validates that the control plane payload contains all required fields
    and ensures correct types and default values.
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    namespace: str = Field(..., description="Kubernetes namespace of the workload.")
    workload: str = Field(..., description="Name of the workload")
    endpoint: str = Field(
        ..., description="Full HTTP URL to probe (e.g., http://service/health)."
    )
    timeout_seconds: int = Field(
        default=3, ge=0, description="Timeout in seconds for each HTTP request."
    )
    requests_per_interval: int = Field(
        default=3,
        ge=1,
        description="Number of parallel HTTP requests to send per interval.",
    )


class EventPayload(BaseModel):
    """Base event payload that allows arbitrary additional fields."""

    model_config = ConfigDict(extra="allow")

    event_name: EventEnum = Field(..., description="Name of the event.")
    type: str = Field(default="event", description="Type of payload")
    phase: ExecutionPhase = Field(
        ..., description="Name of the phase this event belongs to."
    )
    details: Optional[str] = Field(default=None, description="Any detailed message")
    error: Optional[str] = Field(default=None, description="Any error class")
    data: Optional[Dict] = Field(default=None, description="Results of the event.")


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
    workload_status: Optional[Dict] = Field(..., description="Name of the workload.")
