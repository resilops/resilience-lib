from reslib.core.context import get_context, set_context
from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError
from reslib.k8s.schema import WorkloadState
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.validators import QuantitySelection


async def validate_min_remaining_replicas() -> None:
    """
    Validate that terminating a selected number of pods does not violate
    minimum workload availability constraints.

    This function calculates how many pods would remain after applying
    the requested disruption and ensures that:

    - At least one pod remains running.
    - The remaining replica count satisfies the configured
      minimum availability requirement.

    The termination quantity is resolved using the provided selection
    mode (absolute or percentage). If validation succeeds, the computed
    number of pods to terminate is stored in the shared execution context.

    Raises:
        DisruptionExceedMinAvailabilityError:
            If the disruption would terminate all pods or reduce the
            workload below the allowed minimum remaining replicas.
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload

    total = workload.runtime.ready_replicas
    selection = QuantitySelection(
        mode=scenario.template.mode, amount=scenario.template.quantity
    )
    pods_to_terminate = selection.with_total(workload.runtime.ready_replicas)

    remaining = total - pods_to_terminate

    if remaining <= 0:
        raise DisruptionExceedMinAvailabilityError(
            error_code="DISRUPTION_TERMINATES_ALL_REPLICAS",
            message="Requested disruption would terminate all running pods.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "remaining_replicas >= 1",
                "inputs": {
                    "mode": str(scenario.template.mode),
                    "quantity": scenario.template.quantity,
                },
                "observed": {
                    "total_ready_replicas": total,
                    "pods_to_terminate": pods_to_terminate,
                    "remaining_replicas": remaining,
                },
            },
            fix_hint=(
                "Reduce termination quantity or percentage so that "
                "at least one replica remains available."
            ),
            retryable=False,
        )

    if remaining < scenario.template.min_remaining_replicas:
        max_allowed = max(0, total - scenario.template.min_remaining_replicas)
        raise DisruptionExceedMinAvailabilityError(
            error_code="DISRUPTION_BELOW_MIN_REMAINING_REPLICAS",
            message=(
                "Requested disruption violates minimum remaining replicas constraint."
            ),
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": (
                    "remaining_replicas >= "
                    "min_remaining_replicas "
                    f"({scenario.template.min_remaining_replicas})"
                ),
                "inputs": {
                    "mode": str(scenario.template.mode),
                    "quantity": scenario.template.quantity,
                    "min_remaining_replicas": scenario.template.min_remaining_replicas,
                },
                "observed": {
                    "total_ready_replicas": total,
                    "pods_to_terminate": pods_to_terminate,
                    "remaining_replicas": remaining,
                },
                "required": {
                    "min_remaining_replicas": scenario.template.min_remaining_replicas,
                    "max_terminations_allowed": max_allowed,
                },
            },
            fix_hint=(
                f"Reduce termination quantity so that "
                f"`quantity <= {max_allowed}` "
                "or increase workload replica count."
            ),
            retryable=False,
        )

    set_context("pod_termination_count", pods_to_terminate)
