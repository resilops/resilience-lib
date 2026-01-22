from pydantic import BaseModel, Field, model_validator
from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError


class MinAvailabilityPolicy(BaseModel):
    """
    Policy to ensure that a minimum number of pods remain after a planned disruption.

    This policy performs a purely mathematical check:
    it does not inspect Kubernetes objects or PodDisruptionBudgets.

    Raises:
        DisruptionExceedMinAvailabilityError: If terminating the planned number of pods
            would violate the minimum required remaining pods.
    """

    total: int = Field(..., ge=0, description="Total number of ready pods")
    terminate: int = Field(
        ..., ge=0, description="Number of pods selected for termination"
    )
    min_remaining: int = Field(
        1, ge=0, description="Minimum number of pods that must remain after disruption"
    )

    @model_validator(mode="after")
    def validate_minimum(self) -> "MinAvailabilityPolicy":
        """
        Validate that the planned termination does not violate minimum availability.
        Raises:
            DisruptionExceedMinAvailabilityError: If remaining pods after termination
                are zero or below the minimum required.
        """
        remaining = self.total - self.terminate

        if remaining <= 0:
            raise DisruptionExceedMinAvailabilityError(
                f"Cannot terminate all pods; {self.total} pods available, "
                f"{self.terminate} planned for termination."
            )

        if remaining < self.min_remaining:
            raise DisruptionExceedMinAvailabilityError(
                f"At least {self.min_remaining} pods must remain after termination, "
                f"but only {remaining} would remain."
            )

        return self
