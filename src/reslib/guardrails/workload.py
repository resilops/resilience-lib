from reslib.constants import MIN_ROLLING_RESTART_REPLICAS, WorkloadStatusEnum
from reslib.core.context import get_context
from reslib.k8s.exceptions import (
    InsufficientReplicasError,
    WorkloadFaultyError,
    WorkloadNotAvailableError,
    WorkloadReconcilingError,
    WorkloadStatusUnavailableError,
)
from reslib.k8s.schema import WorkloadState
from reslib.schemas.scenario import ResiliencyScenario


async def ensure_workload_steady() -> None:
    """
    Validate that a Kubernetes workload is healthy and ready for disruption.

    Checks performed:
    - Status must be available
    - Workload must not be reconciling
    - Workload must be available (serving traffic)
    - Workload must not be in a faulty state

    Raises:
        WorkloadStatusUnavailableError: If status information is missing
        WorkloadReconcilingError: If the workload is currently reconciling
        WorkloadNotAvailableError: If the workload is not available/stable
        WorkloadFaultyError: If the workload is in a faulty state
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    workload_name = workload.spec.name
    runtime = workload.runtime

    if not runtime:
        raise WorkloadStatusUnavailableError(
            error_code="WORKLOAD_STATUS_UNAVAILABLE",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' does not "
                "have runtime status yet, so readiness cannot be checked."
            ),
            fix_hint="Wait for the workload status to appear, then retry.",
        )

    if runtime.status == WorkloadStatusEnum.reconciling:
        raise WorkloadReconcilingError(
            error_code="WORKLOAD_RECONCILING",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' is still "
                "reconciling, so disruption is blocked for now."
            ),
            fix_hint="Wait for reconciliation to finish, then retry.",
        )

    if runtime.status == WorkloadStatusEnum.unavailable:
        raise WorkloadNotAvailableError(
            error_code="WORKLOAD_NOT_AVAILABLE",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' is not "
                "currently available, so disruption is blocked."
            ),
            fix_hint=(
                "Restore workload availability and health checks before retrying."
            ),
        )

    if runtime.status == WorkloadStatusEnum.degraded:
        raise WorkloadFaultyError(
            error_code="WORKLOAD_FAULTY",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' is in a "
                "degraded state, so disruption is blocked."
            ),
            fix_hint="Resolve the workload failure before retrying.",
        )


async def ensure_minimum_replicas_for_restart() -> None:
    """
    Validate that a workload has enough replicas for a rolling restart.

    A rolling restart needs at least two desired and ready replicas to preserve
    availability while Kubernetes replaces pods.
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    workload_name = workload.spec.name
    desired_replicas = workload.spec.replicas
    ready_replicas = workload.runtime.ready_replicas if workload.runtime else 0

    if (
        desired_replicas < MIN_ROLLING_RESTART_REPLICAS
        or ready_replicas < MIN_ROLLING_RESTART_REPLICAS
    ):
        raise InsufficientReplicasError(
            error_code="INSUFFICIENT_REPLICAS_FOR_ROLLING_RESTART",
            message=(
                f"Workload '{workload_name}' in namespace '{namespace}' needs at "
                f"least {MIN_ROLLING_RESTART_REPLICAS} desired and ready replicas "
                f"for a rolling restart, but has {desired_replicas} desired and "
                f"{ready_replicas} ready replica(s)."
            ),
            fix_hint=(
                "Scale the workload to at least 2 replicas and wait for both "
                "replicas to become ready before retrying."
            ),
        )
