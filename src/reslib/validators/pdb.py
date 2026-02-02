from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError
from reslib.k8s.schema import WorkloadState


def ensure_pdb_not_violated(workload: WorkloadState, disruption_budget: int) -> None:
    """
    Validate that a planned pod disruption respects the Kubernetes PodDisruptionBudget
    (PDB).

    Args:
        workload: The workload state containing current ready replicas and PDB info.
        disruption_budget: Number of pods planned to be disrupted.

    Raises:
        DisruptionExceedMinAvailabilityError: If remaining pods after disruption
            are fewer than allowed by the PDB.
    """
    ready_replicas = workload.status.ready_replicas or 0
    min_available = workload.policies.pdb.min_available

    remaining_pods = ready_replicas - disruption_budget

    if remaining_pods < min_available:
        raise DisruptionExceedMinAvailabilityError(
            f"Planned disruption leaves {remaining_pods} pods, but PDB requires "
            f"at least {min_available} pods."
        )
