from pydantic import BaseModel, ConfigDict, Field


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
    http_request_timeout_seconds: int = Field(
        default=3, ge=0, description="Timeout in seconds for each HTTP request."
    )
    requests_per_interval: int = Field(
        default=3,
        ge=1,
        description="Number of parallel HTTP requests to send per interval.",
    )
