from reslib.constants import ENDPOINT_DRAIN_SCENARIO_TEMPLATE
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

    is_endpoint_drain = scenario.name == ENDPOINT_DRAIN_SCENARIO_TEMPLATE
    pdb_config_exists: bool = workload.policies and workload.policies.pdb is not None

    if not pdb_config_exists:
        if is_endpoint_drain:
            return

        raise PdbNotConfiguredError(
            error_code="PDB_NOT_CONFIGURED",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' does not "
                "have a PodDisruptionBudget."
            ),
            fix_hint="Create a PodDisruptionBudget for this workload.",
        )

    ready_replicas = workload.runtime.ready_replicas or 0

    if is_endpoint_drain:
        disruption_budget = 1
        action_label = "Draining"
    else:
        selection = QuantitySelection(
            mode=scenario.template.mode,
            amount=scenario.template.quantity,
        )
        disruption_budget = selection.with_total(ready_replicas)
        action_label = "Disrupting"

    pdb = workload.policies.pdb
    max_unavailable = pdb.max_unavailable
    if max_unavailable is not None and disruption_budget > max_unavailable:
        raise DisruptionExceedMinAvailabilityError(
            error_code="PDB_MAX_UNAVAILABLE_VIOLATION",
            message=(
                f"{action_label} {disruption_budget} pod(s) from workload "
                f"'{workload_name}' would exceed the PodDisruptionBudget maximum "
                f"unavailable count of {max_unavailable}."
            ),
            fix_hint=(
                "Reduce the planned disruption, increase replicas, or adjust the "
                "PodDisruptionBudget if that is intentional."
            ),
        )

    min_available = pdb.min_available
    if min_available is None:
        return

    remaining_pods = ready_replicas - disruption_budget

    if remaining_pods < min_available:
        raise DisruptionExceedMinAvailabilityError(
            error_code="PDB_MIN_AVAILABLE_VIOLATION",
            message=(
                f"{action_label} {disruption_budget} pod(s) from workload "
                f"'{workload_name}' would leave {remaining_pods} ready pod(s), "
                f"below the PodDisruptionBudget minimum of {min_available}."
            ),
            fix_hint=(
                f"Reduce the planned disruption to at most "
                f"{max(0, ready_replicas - min_available)} pod(s), increase "
                "replicas, or adjust the PodDisruptionBudget if that is intentional."
            ),
        )
