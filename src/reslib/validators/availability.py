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
            "Cannot terminate all pods",
            context={"total": total, "terminate": terminate},
        )

    if remaining < min_remaining:
        raise DisruptionExceedMinAvailabilityError(
            "Does not meet minimum availability criteria",
            context={"remaining": remaining, "min_required": min_remaining},
        )
