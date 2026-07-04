from reslib.k8s.exceptions import WorkloadNotFound


def test_k8s_exceptions_inherit_base_error_behavior():
    error = WorkloadNotFound(
        error_code="WORKLOAD_NOT_FOUND",
        message="Deployment checkout-api was not found",
    )

    assert str(error) == "[WORKLOAD_NOT_FOUND] Deployment checkout-api was not found"
