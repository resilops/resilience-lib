
class WorkloadNotFound(Exception):
    pass


class MultipleWorkloadsReturned(Exception):
    pass


class WorkloadReconcilingError(Exception):
    pass


class DisruptionExceedMinAvailabilityError(Exception):
    pass


class PodDeletionTimeoutError(Exception):
    pass


class PodsSelectionError(Exception):
    pass
