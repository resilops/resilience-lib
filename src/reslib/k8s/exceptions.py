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
