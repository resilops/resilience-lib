from reslib.core.context import get_context
from reslib.guardrails.schema import PDBDisruptionBudgetSchema
from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError
from reslib.k8s.schema import WorkloadState


def ensure_pdb_not_violated(**kwargs) -> None:
    """
    Validate that the planned pod disruption does not violate the workload
    PodDisruptionBudget (PDB) minimum availability constraint.

    This guardrail uses the previously computed `pod_termination_count`
    from execution context and verifies that the number of remaining ready
    replicas stays >= `pdb.min_available`.

    Raises:
        BaseError:
            If PDB checking is enabled and the planned disruption would
            reduce remaining pods below the PDB minimum availability, or
            if required context is missing.
    """
    workload: WorkloadState = get_context("workload")
    args = PDBDisruptionBudgetSchema(**kwargs)
    if args.respect_pdb and workload.policies and not workload.policies.pdb:
        return

    disruption_budget = get_context("pod_termination_count")
    if not disruption_budget:
        raise ValueError("Missing Planned disruption budget")

    ready_replicas = workload.status.ready_replicas or 0
    min_available = workload.policies.pdb.min_available

    remaining_pods = ready_replicas - disruption_budget

    if remaining_pods < min_available:
        raise DisruptionExceedMinAvailabilityError(
            error_code="PDB_MIN_AVAILABLE_VIOLATION",
            message=(
                "Planned disruption would violate PodDisruptionBudget "
                "minimum availability."
            ),
            context={
                "rule": "remaining_ready_replicas >= pdb.min_available",
                "inputs": {
                    "respect_pdb": args.respect_pdb,
                    "pod_termination_count": disruption_budget,
                },
                "observed": {
                    "ready_replicas": ready_replicas,
                    "remaining_ready_replicas": remaining_pods,
                    "pdb_min_available": min_available,
                },
                "required": {
                    "pdb_min_available": min_available,
                    "max_terminations_allowed": max(0, ready_replicas - min_available),
                },
            },
            fix_hint=(
                "Reduce `pod_termination_count` so remaining replicas "
                "stay >= PDB minAvailable, "
                "or adjust the PDB policy if this disruption is intended."
            ),
            retryable=False,
        )
