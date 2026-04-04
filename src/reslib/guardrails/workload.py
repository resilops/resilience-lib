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
                "Workload runtime state is missing; cannot determine readiness for "
                "disruption."
            ),
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "workload.runtime is not None",
                "inputs": {"workload_name": workload.spec.name},
                "observed": {"runtime_present": False},
            },
            fix_hint=(
                "Ensure the workload controller reports runtime state and retry "
                "once runtime state is available."
            ),
            retryable=True,
        )

    if runtime.status == WorkloadStatusEnum.reconciling:
        raise WorkloadReconcilingError(
            error_code="WORKLOAD_RECONCILING",
            message="Workload is currently reconciling; disruption is blocked.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "workload.runtime.reconciling is False",
                "inputs": {"workload_name": workload.spec.name},
                "observed": {"reconciling": True},
            },
            fix_hint="Wait for reconciliation to complete, then retry the disruption.",
            retryable=True,
        )

    if not runtime.status == WorkloadStatusEnum.unavailable:
        raise WorkloadNotAvailableError(
            error_code="WORKLOAD_NOT_AVAILABLE",
            message="Workload is not available/stable; disruption is blocked.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "workload.runtime.is_available is True",
                "inputs": {"workload_name": workload.spec.name},
                "observed": {"is_available": False},
            },
            fix_hint=(
                "Restore workload availability (investigate readiness/health checks) "
                "before running disruption."
            ),
            retryable=True,
        )

    if runtime.status == WorkloadStatusEnum.degraded:
        raise WorkloadFaultyError(
            error_code="WORKLOAD_FAULTY",
            message="Workload is in a faulty state; disruption is blocked.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "workload.runtime.is_faulty is False",
                "inputs": {"workload_name": workload.spec.name},
                "observed": {"is_faulty": True},
            },
            fix_hint=(
                "Resolve the underlying fault condition before running disruption."
            ),
            retryable=False,
        )
