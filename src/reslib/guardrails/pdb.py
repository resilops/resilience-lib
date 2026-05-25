from reslib.core.context import get_context
from reslib.k8s.exceptions import (
    DisruptionExceedMinAvailabilityError,
    PdbNotConfiguredError,
)
from reslib.k8s.schema import WorkloadState
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.validators import QuantitySelection


async def ensure_pdb_not_violated(**kwargs) -> None:  # noqa
    """
    Validate that the planned pod disruption does not violate the workload
    PodDisruptionBudget (PDB) minimum availability constraint.

    This guardrail resolves the planned pod disruption from the scenario
    template and verifies that the number of remaining ready replicas stays
    >= `pdb.min_available`.

    Raises:
        BaseError:
            If PDB checking is enabled and the planned disruption would
            reduce remaining pods below the PDB minimum availability, or
            if required context is missing.
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload

    pdb_config_exists: bool = workload.policies and workload.policies.pdb is not None

    if not pdb_config_exists:
        raise PdbNotConfiguredError(
            error_code="PDB_NOT_CONFIGURED",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' does not "
                "have a PodDisruptionBudget."
            ),
            fix_hint="Create a PodDisruptionBudget for this workload.",
        )

    ready_replicas = workload.runtime.ready_replicas or 0
    selection = QuantitySelection(
        mode=scenario.template.mode,
        amount=scenario.template.quantity,
    )
    disruption_budget = selection.with_total(ready_replicas)
    min_available = workload.policies.pdb.min_available

    remaining_pods = ready_replicas - disruption_budget

    if remaining_pods < min_available:
        raise DisruptionExceedMinAvailabilityError(
            error_code="PDB_MIN_AVAILABLE_VIOLATION",
            message=(
                f"Disrupting {disruption_budget} pod(s) would leave {remaining_pods} "
                f"ready pod(s), below the PodDisruptionBudget minimum of "
                f"{min_available}."
            ),
            fix_hint=(
                f"Reduce pod terminations to at most "
                f"{max(0, ready_replicas - min_available)}, or adjust the "
                "PodDisruptionBudget if that is intentional."
            ),
        )
