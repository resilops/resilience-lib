from pydantic import BaseModel, Field, ConfigDict
from reslib import helpers as h


class MeasureHTTPLatencyArgs(BaseModel):
    """
    Arguments for the `measure_http_latency` function.

    Validates that the control plane payload contains all required fields
    and ensures correct types and default values.
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    namespace: str = Field(
        ..., description="Kubernetes namespace of the workload."
    )
    labels: str = Field(
        ..., description="Label selector to identify the workload pods."
    )
    endpoint: str = Field(
        ..., description="Full HTTP URL to probe (e.g., http://service/health)."
    )
    timeout: int = Field(
        3, ge=0, description="Timeout in seconds for each HTTP request."
    )
    requests_per_interval: int = Field(
        5, ge=1, description="Number of parallel HTTP requests to send per interval."
    )
    event_recorder: h.BaseEventRecorder = Field(
        default_factory=h.NoopEventRecorder,
        description="Async recorder used to emit latency metrics."
    )
