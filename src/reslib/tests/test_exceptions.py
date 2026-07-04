from reslib.exceptions import BaseError, FunctionNotFound, NotSupportedError


def test_base_error_formats_and_serializes():
    error = BaseError(
        error_code="invalid_state",
        message="State transition failed",
        fix_hint="Retry after rollout completes",
    )

    assert str(error) == "[invalid_state] State transition failed"
    assert error.to_dict() == {
        "type": "BaseError",
        "error_code": "invalid_state",
        "message": "State transition failed",
        "fix_hint": "Retry after rollout completes",
    }


def test_base_error_omits_empty_fix_hint():
    error = NotSupportedError(
        error_code="unsupported",
        message="Feature not available",
    )

    assert error.to_dict() == {
        "type": "NotSupportedError",
        "error_code": "unsupported",
        "message": "Feature not available",
    }


def test_plain_marker_exceptions_behave_like_exceptions():
    assert isinstance(FunctionNotFound(), Exception)
