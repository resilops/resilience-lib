from typing import Optional

from pydantic import BaseModel, Field

from reslib.constants import (
    HPA_CPU_STRESS_SCENARIO_TEMPLATE,
    POD_EVICTION_SCENARIO_TEMPLATE,
    POD_RECOVERY_SCENARIO_TEMPLATE,
    ROLLING_RESTART_SCENARIO_TEMPLATE,
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
    QuantitySelectionModeEnum,
)


class BaseScenarioTemplate(BaseModel):
    """
    Base template for all resiliency scenarios.

    This model defines the minimal required context for executing a scenario
    against a Kubernetes workload. All concrete scenario templates extend
    this base and add disruption-specific configuration fields.

    Attributes:
        namespace: Kubernetes namespace where the scenario will execute.
        workload: Target workload name (typically a Deployment).
    """

    namespace: str = Field(
        ...,
        description="Kubernetes namespace in which the scenario will be executed.",
    )
    workload: str = Field(
        ...,
        description="Name of the target workload (e.g., Deployment) for the scenario.",
    )


class PodRecoveryTemplate(BaseScenarioTemplate):
    """
    Template for a single-workload pod termination scenario.

    This scenario simulates an unexpected pod failure by terminating
    one or more pods belonging to a workload. It validates that
    Kubernetes self-healing mechanisms restore availability while
    respecting safety constraints such as minimum replicas and PDB rules.

    Attributes:
        quantity: Number of pods to terminate. Must be greater than zero.
        mode: How the quantity is interpreted:
            - 'absolute' → terminate a fixed number of pods.
            - 'percentage' → terminate a percentage of replicas.
        min_remaining_replicas: Minimum number of pods that must remain
            available after termination to prevent total outage.
    """

    quantity: int = Field(
        ...,
        gt=0,
        description="Number of pods to terminate. Must be greater than zero.",
    )

    mode: QuantitySelectionModeEnum = Field(
        ...,
        description="Quantity selection mode: 'absolute' or 'percentage'.",
    )

    min_remaining_replicas: int = Field(
        default=1,
        ge=1,
        description=(
            "Minimum number of replicas that must remain running "
            "after pod termination to ensure service continuity."
        ),
    )


class PodEvictionTemplate(PodRecoveryTemplate):
    """
    Template for a voluntary pod eviction scenario.

    This scenario uses the Kubernetes eviction API to remove one or more pods
    while respecting disruption policy. It shares the same pod selection and
    minimum remaining replica controls as pod recovery, but remains a distinct
    template type for schema clarity.
    """


class RollingRestartTemplate(BaseScenarioTemplate):
    """
    Template for a rolling restart scenario.

    Rolling restart only needs the target namespace and workload. The concrete
    type keeps scenario-template mapping explicit without reusing the base class
    as a runnable scenario template.
    """


class HpaCpuStressTemplate(BaseScenarioTemplate):
    """
    Template for CPU-based Horizontal Pod Autoscaler (HPA) stress testing.

    This scenario artificially increases CPU utilization in order to
    trigger HPA scaling behavior. It validates that the workload scales
    up under load and stabilizes once stress is removed.

    Attributes:
        metric_source: HPA metric type used for scaling (e.g., RESOURCE).
        resource_type: Resource targeted for scaling (e.g., 'cpu').
        idle_cpu_pct: Estimated baseline CPU utilization percentage per pod
            under normal conditions.
        cpu_stress_threshold_pct: Target CPU utilization percentage applied
            during stress. Capped to prevent unintended pod crashes.
        min_idle_pct: Percentage of pods intentionally excluded from stress
            to avoid full saturation and preserve partial availability.
    """

    container_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of the container to run the stress test, if not given first "
            "container will be selected."
        ),
    )
    metric_source: HpaMetricSourceEnum = Field(
        ...,
        description="HPA metric source type used for scaling (e.g., RESOURCE).",
    )

    resource_type: HpaResourceTypeEnum = Field(
        ...,
        description="Specific resource name used in the HPA metric (e.g., 'cpu').",
    )

    idle_cpu_pct: int = Field(
        default=10,
        ge=0,
        le=100,
        description=(
            "Estimated baseline CPU utilization percentage per pod "
            "under normal (non-stress) conditions."
        ),
    )

    cpu_stress_threshold_pct: int = Field(
        default=95,
        gt=0,
        le=95,  # Cap stress to avoid destabilizing pods
        description=(
            "Target CPU utilization percentage to apply during stress testing. "
            "Upper bound is capped to reduce risk of pod crashes."
        ),
    )

    min_idle_pct: int = Field(
        default=20,
        ge=0,
        le=100,
        description=(
            "Percentage of pods to exclude from stress injection "
            "to maintain partial service availability."
        ),
    )


SCENARIO_TEMPLATES_MAPPING = {
    POD_RECOVERY_SCENARIO_TEMPLATE: PodRecoveryTemplate,
    POD_EVICTION_SCENARIO_TEMPLATE: PodEvictionTemplate,
    ROLLING_RESTART_SCENARIO_TEMPLATE: RollingRestartTemplate,
    HPA_CPU_STRESS_SCENARIO_TEMPLATE: HpaCpuStressTemplate,
}
