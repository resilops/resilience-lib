from pydantic import BaseModel, Field, model_validator
from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError


class PodDisruptionBudgetPolicy(BaseModel):
    """
    Policy that ensures planned pod disruptions do not violate the
    Kubernetes PodDisruptionBudget (PDB).

    Raises:
        DisruptionExceedMinAvailabilityError: If the disruption would leave fewer
            pods than allowed by the PDB.
    """

    remaining_pods: int = Field(
        ...,
        ge=0,
        description="Number of pods that will remain after planned disruption",
    )
    pdb_min_available: int = Field(
        ...,
        ge=0,
        description="Minimum pods required according to PodDisruptionBudget",
    )

    @model_validator(mode="after")
    def check_pdb_compliance(self) -> "PodDisruptionBudgetPolicy":
        """
        Validate that remaining pods respect the PodDisruptionBudget.

        Raises:
            DisruptionExceedMinAvailabilityError: If remaining pods are less than
            PDB requirement.
        """
        if self.remaining_pods < self.pdb_min_available:
            raise DisruptionExceedMinAvailabilityError(
                f"Planned disruption leaves {self.remaining_pods} pods, "
                f"but PDB requires at least {self.pdb_min_available} pods."
            )
        return self
