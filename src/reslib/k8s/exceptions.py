from reslib.exceptions import ExceptionWithContext


class WorkloadNotFound(ExceptionWithContext):
    pass


class WorkloadStatusUnavailableError(ExceptionWithContext):
    pass


class WorkloadReconcilingError(ExceptionWithContext):
    pass


class WorkloadFaultyError(ExceptionWithContext):
    pass


class WorkloadNotAvailableError(ExceptionWithContext):
    pass


class DisruptionExceedMinAvailabilityError(ExceptionWithContext):
    pass


class PodDeletionTimeoutError(ExceptionWithContext):
    pass


class PodsSelectionError(ExceptionWithContext):
    pass


class WorkloadAtMaxError(ExceptionWithContext):
    pass


class HpaNotConfiguredError(ExceptionWithContext):
    pass


class MetricsServerUnavailableError(ExceptionWithContext):
    pass


class HpaMetricsNotFoundError(ExceptionWithContext):
    pass


class PodsToStressExceededError(ExceptionWithContext):
    pass


class CPUStressCommandFailed(ExceptionWithContext):
    pass


class ContainerCrashedError(ExceptionWithContext):
    pass


class InsufficientMemoryError(ExceptionWithContext):
    pass


class HpaScaledError(ExceptionWithContext):
    pass


class ReachedDesiredReplicaError(ExceptionWithContext):
    pass


class ReplicasRestoredError(ExceptionWithContext):
    pass
