from reslib.core.context import get_context, set_context
from reslib.guardrails.schema import MinRemainingReplicasSchema
from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError
from reslib.k8s.schema import WorkloadState
from reslib.schemas.validators import QuantitySelection


def validate_min_remaining_replicas(**kwargs) -> None:
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

    Args:
        **kwargs:
            Parameters used to construct ``MinRemainingReplicasSchema``:
            quantity, mode, and minimum remaining replicas configuration.

    Raises:
        DisruptionExceedMinAvailabilityError:
            If the disruption would terminate all pods or reduce the
            workload below the allowed minimum remaining replicas.
    """
    workload: WorkloadState = get_context("workload")
    args = MinRemainingReplicasSchema(**kwargs)
    total = workload.status.ready_replicas
    selection = QuantitySelection(mode=args.mode, amount=args.quantity)
    pods_to_terminate = selection.with_total(workload.status.ready_replicas)

    remaining = total - pods_to_terminate

    if remaining <= 0:
        raise DisruptionExceedMinAvailabilityError(
            error_code="DISRUPTION_TERMINATES_ALL_REPLICAS",
            message="Requested disruption would terminate all running pods.",
            context={
                "rule": "remaining_replicas >= 1",
                "inputs": {
                    "mode": str(args.mode),
                    "quantity": args.quantity,
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

    if remaining < args.min_remaining_replicas:
        max_allowed = max(0, total - args.min_remaining_replicas)
        raise DisruptionExceedMinAvailabilityError(
            error_code="DISRUPTION_BELOW_MIN_REMAINING_REPLICAS",
            message=(
                "Requested disruption violates minimum remaining replicas constraint."
            ),
            context={
                "rule": (
                    "remaining_replicas >= "
                    f"min_remaining_replicas ({args.min_remaining_replicas})"
                ),
                "inputs": {
                    "mode": str(args.mode),
                    "quantity": args.quantity,
                    "min_remaining_replicas": args.min_remaining_replicas,
                },
                "observed": {
                    "total_ready_replicas": total,
                    "pods_to_terminate": pods_to_terminate,
                    "remaining_replicas": remaining,
                },
                "required": {
                    "min_remaining_replicas": args.min_remaining_replicas,
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
