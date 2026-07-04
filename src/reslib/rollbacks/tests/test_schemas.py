import pytest
from pydantic import ValidationError

from reslib.rollbacks.schemas import (
    EndpointRestoreTimeout,
    HpaScaleDownSchema,
    RollingRestartTimeout,
)


def test_hpa_scale_down_schema_uses_expected_default():
    schema = HpaScaleDownSchema()

    assert schema.timeout_seconds == 500


def test_rolling_restart_timeout_enforces_upper_bound():
    with pytest.raises(ValidationError):
        RollingRestartTimeout(timeout_seconds=1801)


def test_endpoint_restore_timeout_accepts_valid_timeout():
    schema = EndpointRestoreTimeout(timeout_seconds=300)

    assert schema.timeout_seconds == 300
