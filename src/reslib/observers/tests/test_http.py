from reslib.observers.http import (
    LATENCY_BUCKET_BOUNDS_MS,
    _build_latency_buckets,
    _build_measurement,
    _get_request_group_timeout,
)


def test_build_latency_buckets_handles_empty_samples():
    buckets = _build_latency_buckets([])

    assert buckets["le_5"] == 0
    assert buckets[f"gt_{LATENCY_BUCKET_BOUNDS_MS[-1]}"] == 0


def test_build_latency_buckets_counts_cumulative_ranges():
    buckets = _build_latency_buckets([7, 18, 42, 1200])

    assert buckets["le_10"] == 1
    assert buckets["le_20"] == 2
    assert buckets["le_50"] == 3
    assert buckets[f"gt_{LATENCY_BUCKET_BOUNDS_MS[-1]}"] == 0


def test_build_measurement_aggregates_transport_errors_without_latencies():
    measurement = _build_measurement(
        timed_responses=[],
        transport_error_count=2,
        interval_start="2026-01-01T00:00:00+00:00",
        interval_end="2026-01-01T00:00:05+00:00",
    )

    assert measurement["request_count"] == 2
    assert measurement["error_count_total"] == 2
    assert measurement["latency_ms_sum"] == 0.0


def test_get_request_group_timeout_scales_with_parallelism():
    assert (
        _get_request_group_timeout(
            request_timeout_seconds=3,
            requests_per_interval=5,
        )
        == 15
    )
