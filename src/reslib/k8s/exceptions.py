from reslib.exceptions import BaseError


class WorkloadNotFound(BaseError):
    pass


class WorkloadStatusUnavailableError(BaseError):
    pass


class WorkloadReconcilingError(BaseError):
    pass


class WorkloadFaultyError(BaseError):
    pass


class WorkloadNotAvailableError(BaseError):
    pass


class DisruptionExceedMinAvailabilityError(BaseError):
    pass


class PodDeletionTimeoutError(BaseError):
    pass


class PodEvictionTimeoutError(BaseError):
    pass


class PodsSelectionError(BaseError):
    pass


class WorkloadAtMaxError(BaseError):
    pass


class HpaNotConfiguredError(BaseError):
    pass


class PdbNotConfiguredError(BaseError):
    pass


class MetricsServerUnavailableError(BaseError):
    pass


class HpaMetricsNotFoundError(BaseError):
    pass


class PodsToStressExceededError(BaseError):
    pass


class CPUStressCommandFailed(BaseError):
    pass


class ContainerCrashedError(BaseError):
    pass


class InsufficientMemoryError(BaseError):
    pass


class HpaScalePodReadyError(BaseError):
    pass


class HpaScaleDetectedError(BaseError):
    pass


class ReachedDesiredReplicaError(BaseError):
    pass


class ReplicasRestoredError(BaseError):
    pass
