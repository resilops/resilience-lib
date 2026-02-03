from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from reslib import helpers as h
from reslib.constants import (
    SUPPORTED_HPA_METRIC_TYPES,
    SUPPORTED_HPA_RESOURCE_NAMES,
    HpaMetricTypeEnum,
    HpaResourceNameEnum,
)
from reslib.exceptions import NotSupportedError


class HpaCPUStressArgsTemplate(BaseModel):
    """
    Arguments for generating CPU load on pods to trigger
    Horizontal Pod Autoscaler (HPA) scale-up based on CPU utilization.

    This model defines user-configurable CPU assumptions used to
    safely calculate and apply CPU stress without causing downtime.
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    namespace: str = Field(..., description="Kubernetes namespace of the workload.")
    workload: str = Field(..., description="Name of the workload")
    container_name: Optional[str] = Field(
        default=None,
        description=(
            "Name of the container to run the stress test, if not given first "
            "container will be selected."
        ),
    )
    metric_type: HpaMetricTypeEnum = Field(
        ...,
        description="Type of HPA metric to test scaling against (e.g., CPU, memory).",
    )
    resource: HpaResourceNameEnum = Field(
        ...,
        description="Specific resource name for the metric (e.g., 'cpu', 'memory').",
    )
    idle_cpu_pct: int = Field(
        default=10,
        ge=0,
        le=100,
        description="Estimated CPU percentage used by a pod when idle (baseline).",
    )
    max_cpu_stress_pct_per_pod: int = Field(
        default=95,
        gt=0,
        le=95,  # Cap stress to avoid pod down
        description="Target CPU percentage to stress each pod to during the test.",
    )
    min_pods_idle_pct: int = Field(
        default=20, le=100, description="Exclude % pods from stress tests"
    )
    max_stress_duration: int = Field(
        120,
        ge=30,
        le=600,  # Some upper limit.
        description=(
            "A new ready replica must be observed within this duration after CPU "
            "stress begins. Default is 120s."
        ),
    )

    telemetry: h.BaseTelemetry = Field(
        default_factory=h.NoopTelemetry,
        description="Telemetry recorder to log metrics.",
    )

    @model_validator(mode="after")
    def validate_metric_and_resource(self) -> "HpaCPUStressArgsTemplate":
        """
        Validate that only supported metric types and resources are used.

        Raises:
            NotSupportedError: If the metric type or resource is not supported.
        """
        if self.metric_type not in SUPPORTED_HPA_METRIC_TYPES:
            raise NotSupportedError(
                f"HPA scaling tests for metric type "
                f"'{self.metric_type}' are not supported yet."
            )

        if self.resource not in SUPPORTED_HPA_RESOURCE_NAMES:
            raise NotSupportedError(
                f"HPA scaling tests for resource '{self.resource}' "
                "are not supported yet."
            )

        return self
