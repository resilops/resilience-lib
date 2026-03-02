from pydantic import BaseModel, Field

from reslib.constants import (
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
    QuantitySelectionModeEnum,
)


class MinRemainingReplicasSchema(BaseModel):
    """
    Configuration defining how many replicas may be removed while ensuring
    a minimum number of pods remain running.

    This schema is typically used during disruption or stress operations
    where pod termination must not reduce workload availability below
    a safe threshold.
    """

    quantity: int = Field(..., gt=0, description="Number of pods to terminate (>=0).")
    mode: QuantitySelectionModeEnum = Field(
        ..., description="Quantity selection mode: 'absolute' or 'percentage'."
    )
    min_remaining_replicas: int = Field(
        default=1,
        ge=1,
        description="Minimum number of pods that must remain after deletion.",
    )


class PDBDisruptionBudgetSchema(BaseModel):
    """
    Controls whether Kubernetes PodDisruptionBudget (PDB) constraints
    should be respected during pod disruption operations.

    When enabled, disruptions will only proceed if they comply with
    existing PDB availability guarantees.
    """

    respect_pdb: bool = Field(
        default=True,
        description="Whether to enforce PodDisruptionBudget rules.",
    )


class HPAResourceMetricSchema(BaseModel):
    """
    Base schema describing the Horizontal Pod Autoscaler (HPA) resource
    metric used for scaling validation or stress testing.

    Defines which metric source and resource type should be evaluated
    when simulating or verifying autoscaling behavior.
    """

    metric_source: HpaMetricSourceEnum = Field(
        ...,
        description="Type of HPA metric to test scaling against (e.g., CPU, memory).",
    )
    resource_type: HpaResourceTypeEnum = Field(
        ...,
        description="Specific resource name for the metric (e.g., 'cpu', 'memory').",
    )


class PodStressSchema(HPAResourceMetricSchema):
    """
    Configuration for applying CPU stress to pods in order to trigger
    or validate Horizontal Pod Autoscaler (HPA) behavior.

    Extends HPAResourceMetricSchema by defining stress intensity,
    expected idle utilization, and exclusion rules for pods that
    should not participate in stress testing.
    """

    idle_cpu_pct: int = Field(
        default=10,
        ge=0,
        le=100,
        description="Estimated CPU percentage used by a pod when idle (baseline).",
    )

    cpu_stress_threshold_pct: int = Field(
        default=95,
        gt=0,
        le=95,  # Cap stress to avoid pod down
        description="Target CPU percentage to stress each pod to during the test.",
    )

    min_idle_pct: int = Field(
        default=20,
        le=100,
        description="Exclude % pods from stress tests",
    )
