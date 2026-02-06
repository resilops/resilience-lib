from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from reslib.constants import (
    SUPPORTED_HPA_METRIC_SOURCES,
    SUPPORTED_HPA_RESOURCE_TYPES,
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
)
from reslib.exceptions import NotSupportedError


class HpaCPUStressArgsTemplate(BaseModel):
    """
    Arguments for generating CPU load on pods to trigger
    Horizontal Pod Autoscaler (HPA) scale-up based on CPU utilization.

    This model defines user-configurable CPU assumptions used to
    safely calculate and apply CPU stress without causing downtime.
    """

    model_config = ConfigDict(extra="allow")

    namespace: str = Field(..., description="Kubernetes namespace of the workload.")
    workload: str = Field(..., description="Name of the workload")
    container_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of the container to run the stress test, if not given first "
            "container will be selected."
        ),
    )
    metric_source: HpaMetricSourceEnum = Field(
        ...,
        description="Type of HPA metric to test scaling against (e.g., CPU, memory).",
    )
    resource_type: HpaResourceTypeEnum = Field(
        ...,
        description="Specific resource name for the metric (e.g., 'cpu', 'memory').",
    )
    idle_cpu_pct: int = Field(
        default=10,
        ge=0,
        le=100,
        description="Estimated CPU percentage used by a pod when idle (baseline).",
    )
    pod_cpu_stress_threshold_pct: int = Field(
        default=95,
        gt=0,
        le=95,  # Cap stress to avoid pod down
        description="Target CPU percentage to stress each pod to during the test.",
    )
    min_pods_idle_pct: int = Field(
        default=20, le=100, description="Exclude % pods from stress tests"
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
    hpa_scale_down_timeout_seconds: int = Field(
        500,
        le=1200,  # Some upper limit.
        description="Max time it takes for the pods to scale down",
    )

    @model_validator(mode="after")
    def validate_metric_and_resource(self) -> "HpaCPUStressArgsTemplate":
        """
        Validate that only supported metric types and resources are used.

        Raises:
            NotSupportedError: If the metric type or resource is not supported.
        """
        if self.metric_source not in SUPPORTED_HPA_METRIC_SOURCES:
            raise NotSupportedError(
                "HPA scaling tests not supported for a given metrics yet.",
                context={"metric_source": self.metric_source},
            )

        if self.resource_type not in SUPPORTED_HPA_RESOURCE_TYPES:
            raise NotSupportedError(
                "HPA scaling tests not supported for a given resource yet.",
                context={"resource": self.resource},
            )

        return self
