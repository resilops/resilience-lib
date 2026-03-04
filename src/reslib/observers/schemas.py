from pydantic import BaseModel, Field


class HTTPLatencyArgsTemplate(BaseModel):
    """
    Arguments for the `measure_http_latency` function.

    Validates that the control plane payload contains all required fields
    and ensures correct types and default values.
    """

    endpoint: str = Field(
        ..., description="Full HTTP URL to probe (e.g., http://service/health)."
    )
    request_timeout_seconds: int = Field(
        default=3, ge=0, description="Timeout in seconds for each HTTP request."
    )
    requests_per_interval: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of parallel HTTP requests to send per interval.",
    )
