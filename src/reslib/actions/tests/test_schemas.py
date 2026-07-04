import pytest
from pydantic import ValidationError

from reslib.actions.schemas import (
    EndpointDrainSchema,
    PodStressSchema,
    PodTerminationSchema,
)


def test_pod_termination_schema_has_expected_default():
    schema = PodTerminationSchema()

    assert schema.timeout_seconds == 300


def test_pod_stress_schema_enforces_lower_bound():
    with pytest.raises(ValidationError):
        PodStressSchema(max_stress_duration_seconds=10)


def test_endpoint_drain_schema_accepts_custom_timeout():
    schema = EndpointDrainSchema(timeout_seconds=240)

    assert schema.timeout_seconds == 240
