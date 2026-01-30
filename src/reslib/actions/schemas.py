from pydantic import BaseModel, ConfigDict, Field

from reslib import helpers as h
from reslib.constants import QuantitySelectionModeEnum


class TerminatePodsArgs(BaseModel):
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
    telemetry: h.BaseTelemetry = Field(
        default_factory=h.NoopTelemetry,
        description="Telemetry recorder to log metrics.",
    )
