from reslib.constants import WorkloadStatusEnum
from reslib.core.context import get_context
from reslib.k8s.exceptions import (
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
            fix_hint=("Wait for the workload status to appear, then retry."),
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
