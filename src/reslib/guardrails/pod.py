from reslib.k8s.utils import get_single_workload
from reslib.k8s.schema import WorkloadState
from reslib.schemas.validators import QuantitySelection
from reslib.guardrails.schema import ValidatePodTerminationGuardrailArgs
from reslib.policies.workload import WorkloadHealthPolicy
from reslib.policies.availability import MinAvailabilityPolicy
from reslib.policies.pdb import PodDisruptionBudgetPolicy


async def validate_pod_termination_guardrail(**kwargs) -> None:
    """
    Guardrail to validate whether terminating pods is safe for a workload.

    This function enforces multiple policies to ensure disruption safety:
      1. **WorkloadHealthPolicy** — workload must be ready and serving traffic.
      2. **MinAvailabilityPolicy** — a minimum number of pods must remain.
      3. **PodDisruptionBudgetPolicy** — ensures Kubernetes PDB is respected.

    Steps:
      1. Discover workload by namespace and label selector.
      2. Ensure exactly one workload is returned.
      3. Compute the number of pods to terminate using `QuantitySelection`.
      4. Apply all configured validation policies.

    Expected keyword arguments (`**kwargs`):
        namespace: Kubernetes namespace of the workload.
        labels: Label selector to identify the workload pods.
        quantity: Number of pods or percentage to terminate.
        mode: Selection mode, either "absolute" or "percentage".
        respect_pdb: Whether to enforce PodDisruptionBudget rules (default True).
        min_remaining_replicas: Minimum pods that must remain after termination.
        event_recorder: Optional recorder to log events or metrics.

    Raises:
        WorkloadNotFound: If no workloads match the label selector.
        MultipleWorkloadsReturned: If more than one workload matches.
        GuardrailError subclasses: If any policy is violated.
    """

    args = ValidatePodTerminationGuardrailArgs(**kwargs)

    # Discover workloads
    workload: WorkloadState = get_single_workload(
        namespace=args.namespace, labels=args.labels
    )

    # Resolve quantity to terminate
    selection = QuantitySelection(mode=args.mode, amount=args.quantity)
    pods_to_terminate = selection.with_total(workload.status.ready_replicas)

    # validates readiness and reconciling state
    WorkloadHealthPolicy(workload=workload)

    # Apply MinAvailabilityPolicy
    MinAvailabilityPolicy(
        total=workload.status.ready_replicas,
        terminate=pods_to_terminate,
        min_remaining=args.min_remaining_replicas
    )

    # Apply PodDisruptionBudgetPolicy if requested
    if args.respect_pdb:
        pdb_min_available = (
            workload.policies.pdb.min_available
            if workload.policies and workload.policies.pdb
            else 0
        )

        PodDisruptionBudgetPolicy(
            remaining=workload.status.ready_replicas - pods_to_terminate,
            pdb_min_available=pdb_min_available
        )
