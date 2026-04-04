from typing import Dict, Optional

from pydantic import BaseModel, Field

from reslib.constants import EventEnum, MetricsEnum
from reslib.k8s.schema import WorkloadRuntimeState
from reslib.runtime.phases import ExecutionPhase


class EventPayload(BaseModel):
    """Base event payload that allows arbitrary additional fields."""

    event_name: EventEnum = Field(..., description="Name of the event.")
    type: str = Field(default="event", description="Type of payload")
    namespace: str = Field(..., description="Kubernetes namespace")
    workload: str = Field(..., description="Workload name")
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

    metrics_name: MetricsEnum = Field(..., description="Name of the metrics.")
    type: str = Field(default="metrics", description="Type of payload")
    namespace: str = Field(..., description="Kubernetes namespace")
    workload: str = Field(..., description="Workload name")
    function: str = Field(
        ..., description="Name of the function/class that's emitting this metrics."
    )
    is_error: bool = Field(default=False, description="Is error related metrics")
    details: Optional[str] = Field(default=None, description="Any additional details")
    measurement: Optional[Dict] = Field(default=None, description="MMetric measurement")
    workload_status: WorkloadRuntimeState = Field(
        ..., description="Workload runtime state"
    )
