from pydantic import BaseModel, Field


class PodTerminationSchema(BaseModel):
    """Configuration describing max timeout for the pod termination"""

    timeout_seconds: int = Field(
        default=300,
        le=300,
        description="Default timeout for selected the pods termination",
    )


class PodStressSchema(BaseModel):
    """Configuration for Pod stress max duration in seconds."""

    max_stress_duration_seconds: int = Field(
        120,
        ge=30,
        le=600,  # Some upper limit.
        description=(
            "A new ready replica must be observed within this duration after CPU "
            "stress begins. Default is 120s."
        ),
    )
