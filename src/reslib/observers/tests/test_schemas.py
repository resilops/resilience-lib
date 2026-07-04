import pytest
from pydantic import ValidationError

from reslib.observers.schemas import HTTPLatencyArgsTemplate


def test_http_latency_args_template_defaults_are_stable():
    args = HTTPLatencyArgsTemplate(endpoint="https://example.com/health")

    assert args.request_timeout_seconds == 3
    assert args.requests_per_interval == 3


def test_http_latency_args_template_enforces_request_limit():
    with pytest.raises(ValidationError):
        HTTPLatencyArgsTemplate(
            endpoint="https://example.com/health",
            requests_per_interval=11,
        )
