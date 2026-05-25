from pydantic import BaseModel, Field


class HpaScaleDownSchema(BaseModel):
    """
    Configuration defining the expected time window for a workload
    to scale down after a reduction in load or resource pressure.

    This timeout is used to validate that the Horizontal Pod Autoscaler (HPA)
    completes scale-down operations within an acceptable duration.
    """

    timeout_seconds: int = Field(
        500,
        le=1500,  # Some upper limit.
        description="Max time it takes for the pods to scale down",
    )


class PodRespawnTimeout(BaseModel):
    """
    Configuration defining the maximum time allowed for a replacement
    pod to be created and reach a running or ready state after a pod
    termination or disruption event.

    This timeout is typically used to verify workload self-healing
    behavior and ensure that pod recovery occurs within an acceptable
    operational window.
    """

    timeout_seconds: int = Field(
        default=300,
        ge=1,
        description="Timeout to respawn a new pod",
    )


class RollingRestartTimeout(BaseModel):
    """Maximum time allowed for a rolling restart to complete."""

    timeout_seconds: int = Field(
        default=600,
        ge=1,
        le=1800,
        description="Timeout for rolling restart completion",
    )


class EndpointRestoreTimeout(BaseModel):
    """Maximum time allowed for a drained endpoint to be restored."""

    timeout_seconds: int = Field(
        default=120,
        ge=1,
        le=600,
        description="Timeout for a restored pod IP to return to EndpointSlices",
    )
