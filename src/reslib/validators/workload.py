from reslib.k8s.exceptions import (
    WorkloadFaultyError,
    WorkloadNotAvailableError,
    WorkloadReconcilingError,
    WorkloadStatusUnavailableError,
)
from reslib.k8s.schema import WorkloadState


def ensure_workload_steady(workload: WorkloadState) -> None:
    """
    Validate that a Kubernetes workload is healthy and ready for disruption.

    Checks performed:
    - Status must be available
    - Workload must not be reconciling
    - Workload must be available (serving traffic)
    - Workload must not be in a faulty state

    Args:
        workload: The Kubernetes workload to validate

    Raises:
        WorkloadStatusUnavailableError: If status information is missing
        WorkloadReconcilingError: If the workload is currently reconciling
        WorkloadNotAvailableError: If the workload is not available/stable
        WorkloadFaultyError: If the workload is in a faulty state
    """
    status = workload.status

    if not status:
        raise WorkloadStatusUnavailableError(
            f"Workload '{workload.spec.name}' status is unavailable"
        )

    if status.reconciling:
        raise WorkloadReconcilingError(
            f"Workload '{workload.spec.name}' is currently reconciling"
        )

    if not status.is_available:
        raise WorkloadNotAvailableError(
            f"Workload '{workload.spec.name}' is not available/stable"
        )

    if status.is_faulty:
        raise WorkloadFaultyError(f"Workload '{workload.spec.name}' is faulty")
