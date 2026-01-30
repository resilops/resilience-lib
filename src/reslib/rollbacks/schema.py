from pydantic import BaseModel, ConfigDict, Field

from reslib import helpers as h


class WaitForWorkloadStabilityArgs(BaseModel):
    """
    Arguments for the `wait_for_workload_stability` function.

    Validates that the control plane payload contains all required fields
    and ensures correct types and default values.
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    namespace: str = Field(..., description="Kubernetes namespace of the workload.")
    workload: str = Field(..., description="Name of the workload")
    wait_period: int = Field(
        60, ge=10, description="Number of seconds to wait for workload stability."
    )
    telemetry: h.BaseTelemetry = Field(
        default_factory=h.NoopTelemetry,
        description="Telemetry recorder to log metrics.",
    )
