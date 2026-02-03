class WorkloadNotFound(Exception):
    pass


class WorkloadStatusUnavailableError(Exception):
    pass


class WorkloadReconcilingError(Exception):
    pass


class WorkloadFaultyError(Exception):
    pass


class WorkloadNotAvailableError(Exception):
    pass


class DisruptionExceedMinAvailabilityError(Exception):
    pass


class PodDeletionTimeoutError(Exception):
    pass


class PodsSelectionError(Exception):
    pass


class WorkloadAtMaxError(Exception):
    pass


class HpaNotConfiguredError(Exception):
    pass


class MetricsServerUnavailableError(Exception):
    pass


class HpaMetricsNotFoundError(Exception):
    pass


class PodsToStressExceededError(Exception):
    pass


class CPUStressCommandFailed(Exception):
    pass


class ContainerCrashedError(Exception):
    pass


class HpaScaledError(Exception):
    """
    Raised when HPA triggers scaling and replicas increase above the start count.

    Attributes:
        before: Number of replicas before scaling.
        after: Number of replicas after scaling.
    """

    def __init__(self, message: str, *, before: int, after: int):
        super().__init__(message)
        self.before = before
        self.after = after
