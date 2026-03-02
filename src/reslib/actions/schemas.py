from typing import Optional

from pydantic import BaseModel, Field

from reslib.constants import QuantitySelectionModeEnum


class PodTerminationSchema(BaseModel):
    """
    Configuration describing how pod termination should be performed
    during a disruption or resilience test.

    Defines the number of pods to terminate using either an absolute
    value or a percentage of the current workload size, along with
    the maximum time allowed for the selected pods to terminate
    successfully.
    """

    quantity: int = Field(..., gt=0, description="Number of pods to terminate (>=0).")

    mode: QuantitySelectionModeEnum = Field(
        ..., description="Quantity selection mode: 'absolute' or 'percentage'."
    )

    timeout_seconds: int = Field(
        default=300,
        le=300,
        description="Default timeout for selected the pods termination",
    )


class PodStressSchema(BaseModel):
    """
    Configuration defining CPU stress parameters applied to pods in order
    to simulate resource pressure and validate workload resilience or
    autoscaling behavior.

    This schema controls the target CPU utilization during stress execution,
    the assumed idle utilization baseline, the container selected for stress
    injection, and the maximum duration allowed for observing recovery or
    scaling reactions after stress begins.
    """

    cpu_stress_threshold_pct: int = Field(
        default=95,
        gt=0,
        le=95,  # Cap stress to avoid pod down
        description="Target CPU percentage to stress each pod to during the test.",
    )
    idle_cpu_pct: int = Field(
        default=10,
        ge=0,
        le=100,
        description="Estimated CPU percentage used by a pod when idle (baseline).",
    )

    container_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of the container to run the stress test, if not given first "
            "container will be selected."
        ),
    )

    max_stress_duration_seconds: int = Field(
        120,
        ge=30,
        le=600,  # Some upper limit.
        description=(
            "A new ready replica must be observed within this duration after CPU "
            "stress begins. Default is 120s."
        ),
    )
