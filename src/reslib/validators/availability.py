from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError


def validate_min_remaining_replicas(
    total: int, terminate: int, min_remaining: int = 1
) -> None:
    """
    Validate that terminating a given number of pods does not violate
    the minimum required remaining pods.

    Args:
        total: Total number of ready pods.
        terminate: Number of pods planned for termination.
        min_remaining: Minimum number of pods that must remain after disruption.

    Raises:
        DisruptionExceedMinAvailabilityError: If termination would leave fewer
            than `min_remaining` pods or all pods terminated.
    """
    remaining = total - terminate

    if remaining <= 0:
        raise DisruptionExceedMinAvailabilityError(
            f"Cannot terminate all pods; {total} pods available, "
            f"{terminate} planned for termination."
        )

    if remaining < min_remaining:
        raise DisruptionExceedMinAvailabilityError(
            f"At least {min_remaining} pods must remain after termination, "
            f"but only {remaining} would remain."
        )
