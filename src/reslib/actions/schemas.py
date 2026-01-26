from pydantic import BaseModel, Field, ConfigDict
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

    namespace: str = Field(
        ..., description="Kubernetes namespace of the workload."
    )
    labels: str = Field(
        ..., description="Label selector to identify the workload pods."
    )
    quantity: int = Field(
        ..., gt=0, description="Number of pods to terminate (>=0)."
    )
    mode: QuantitySelectionModeEnum = Field(
        ..., description="Quantity selection mode: 'absolute' or 'percentage'."
    )
    event_recorder: h.BaseEventRecorder = Field(
        default_factory=h.NoopEventRecorder, description="Event recorder to log events."
    )
