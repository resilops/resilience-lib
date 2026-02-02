import logging

from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import get_workload
from reslib.schemas.pod import PodTerminationArgsTemplate
from reslib.schemas.validators import QuantitySelection
from reslib.validators.availability import validate_min_remaining_replicas
from reslib.validators.hpa import ensure_not_at_max_replicas
from reslib.validators.pdb import ensure_pdb_not_violated
from reslib.validators.workload import ensure_workload_steady

logger = logging.getLogger(__name__)


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
        event_handler: Optional recorder to log events or metrics.

    Raises:
        WorkloadNotFound: If no workloads match the label selector.
        MultipleWorkloadsReturned: If more than one workload matches.
        GuardrailError subclasses: If any policy is violated.
    """
    logger.info("Validating the pod termination guardrail")
    args = PodTerminationArgsTemplate(**kwargs)

    # Discover workloads
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)
    # Resolve quantity to terminate
    selection = QuantitySelection(mode=args.mode, amount=args.quantity)
    pods_to_terminate = selection.with_total(workload.status.ready_replicas)

    # validates readiness and reconciling state
    ensure_workload_steady(workload=workload)

    # Check if workload is at max
    if workload.spec.hpa:
        ensure_not_at_max_replicas(workload=workload)

    # Apply MinRemainingReplicasPolicy
    validate_min_remaining_replicas(
        total=workload.status.ready_replicas,
        terminate=pods_to_terminate,
        min_remaining=args.min_remaining_replicas,
    )

    # Apply PodDisruptionBudgetPolicy if requested
    if args.respect_pdb and workload.policies and workload.policies.pdb:
        ensure_pdb_not_violated(workload=workload, disruption_budget=pods_to_terminate)

    logger.info("Pod termination guardrail passed")
