from pydantic import BaseModel, Field, ConfigDict
from reslib import helpers as h


class WaitForWorkloadStabilityArgs(BaseModel):
    """
    Arguments for the `wait_for_workload_stability` function.

    Validates that the control plane payload contains all required fields
    and ensures correct types and default values.
    """

    model_config = ConfigDict(extra="allow")  # Allow extra kwargs without error

    namespace: str = Field(
        ...,
        description="Kubernetes namespace of the workload."
    )
    labels: str = Field(
        ...,
        description="Label selector to identify the workload."
    )
    wait_period: int = Field(
        60,
        ge=10,
        description="Number of seconds to wait for workload stability."
    )
    event_recorder: h.BaseEventRecorder = Field(
        default_factory=h.NoopEventRecorder,
        description="Async recorder used to emit metrics or status events."
    )
