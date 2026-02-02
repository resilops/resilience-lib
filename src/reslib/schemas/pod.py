from pydantic import BaseModel, ConfigDict, Field

from reslib import helpers as h
from reslib.constants import QuantitySelectionModeEnum


class PodTerminationArgsTemplate(BaseModel):
    """
    Arguments for the `terminate_pods` function.

    Validates that the control plane payload contains all required fields
    and ensures correct types and values. Extra fields are allowed for
    forward compatibility.
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    namespace: str = Field(..., description="Kubernetes namespace of the workload.")
    workload: str = Field(..., description="Name of the workload")
    quantity: int = Field(..., gt=0, description="Number of pods to terminate (>=0).")
    mode: QuantitySelectionModeEnum = Field(
        ..., description="Quantity selection mode: 'absolute' or 'percentage'."
    )
    min_remaining_replicas: int = Field(
        default=1,
        ge=1,
        description="Minimum number of pods that must remain after deletion.",
    )
    respect_pdb: bool = Field(
        default=True, description="Whether to enforce PodDisruptionBudget rules."
    )
    wait_for_stability: int = Field(
        default=60,
        ge=1,
        description="Wait after pod termination to stabilize workload",
    )
    telemetry: h.BaseTelemetry = Field(
        default_factory=h.NoopTelemetry,
        description="Telemetry recorder to log metrics.",
    )
