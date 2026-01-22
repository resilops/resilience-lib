from pydantic import BaseModel, model_validator
from reslib.k8s.schema import WorkloadState
from reslib.k8s.exceptions import WorkloadReconcilingError


class WorkloadHealthPolicy(BaseModel):
    """
    Policy to ensure that a Kubernetes workload is healthy and ready
    for disruption.

    Attributes:
        workload (WorkloadState): The workload to validate.

    Raises:
        WorkloadReconcilingError: If the workload is not ready for disruption.
            This includes cases where status is unavailable, the workload
            is reconciling, or it is not serving traffic.
    """

    workload: WorkloadState

    @model_validator(mode="after")
    def validate_health(self) -> "WorkloadHealthPolicy":
        """
        Validate that the workload is in a healthy state.

        Raises:
            WorkloadReconcilingError: If the workload is reconciling, not serving
            traffic, or status information is missing.
        """
        status = self.workload.status

        if not status:
            raise WorkloadReconcilingError(
                f"Workload '{self.workload.spec.name}' status is unavailable"
            )

        if status.reconciling:
            raise WorkloadReconcilingError(
                f"Workload '{self.workload.spec.name}' is currently reconciling"
            )

        if status.serving_traffic is False:
            raise WorkloadReconcilingError(
                f"Workload '{self.workload.spec.name}' is not serving traffic"
            )

        return self
