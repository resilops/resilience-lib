import asyncio
from collections import deque
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from reslib.core.context import scenario_context
from reslib.k8s.schema import WorkloadRuntimeState
from reslib.observers import http as http_observer


class TelemetrySpy:
    def __init__(self):
        self.metrics = []

    def emit_event(self, *, event):
        raise AssertionError("events not expected")

    def emit_metrics(self, *, metrics):
        self.metrics.append(metrics)


def _timed_response(status_code=200, latency=12.5):
    request = httpx.Request("GET", "https://example.com/health")
    response = httpx.Response(status_code, request=request)
    return SimpleNamespace(
        response=response,
        latency=latency,
        timestamp=datetime.now(timezone.utc),
    )


def _scenario():
    return SimpleNamespace(
        template=SimpleNamespace(namespace="payments", workload="checkout")
    )


def _runtime_state():
    return WorkloadRuntimeState(
        ready_replicas=3,
        status="healthy",
    )


def test_build_http_metric_payload_includes_error_samples():
    payload = http_observer._build_http_metric_payload(
        state=_runtime_state(),
        timed_responses=[_timed_response(200, 10.0), _timed_response(503, 20.0)],
        error_samples=[RuntimeError("boom")],
        transport_error_count=1,
        interval_start="2026-07-07T12:00:00+00:00",
        interval_end="2026-07-07T12:00:05+00:00",
    )

    assert payload.is_error is True
    assert payload.error == "RuntimeError"
    assert payload.data == {"errors": ["boom"]}
    assert payload.measurement["request_count"] == 3
    assert payload.measurement["http_error_count"] == 1


@pytest.mark.asyncio
async def test_collect_interval_requests_uses_watch_task_group(monkeypatch):
    captured = {}

    async def fake_watch_task_group(*, tasks, timeout, **kwargs):
        captured["timeout"] = timeout
        captured["task_names"] = [name for _, name in tasks]
        for coro, _ in tasks:
            coro.close()
        return ["done"]

    monkeypatch.setattr(http_observer, "watch_task_group", fake_watch_task_group)
    monkeypatch.setattr(
        http_observer.h,
        "send_timed_request",
        AsyncMock(return_value=_timed_response()),
    )

    results = await http_observer._collect_interval_requests(
        endpoint="https://example.com/health",
        request_timeout_seconds=2,
        requests_per_interval=3,
    )

    assert results == ["done"]
    assert captured["timeout"] == 6
    assert captured["task_names"] == ["request:0", "request:1", "request:2"]


@pytest.mark.asyncio
async def test_split_request_results_counts_successes_and_errors():
    async def ok():
        return _timed_response()

    async def fail():
        raise httpx.ConnectError("boom")

    ok_task = asyncio.create_task(ok())
    fail_task = asyncio.create_task(fail())
    done, _ = await asyncio.wait({ok_task, fail_task})

    responses, errors, error_count = http_observer._split_request_results(list(done))

    assert len(responses) == 1
    assert error_count == 1
    assert len(errors) == 1
    assert isinstance(errors[0], httpx.ConnectError)


@pytest.mark.asyncio
async def test_get_workload_state_reads_deployment_and_builds_runtime(monkeypatch):
    fake_k8s = SimpleNamespace(
        read_namespaced_deployment=AsyncMock(return_value="deployment")
    )
    monkeypatch.setattr(
        http_observer,
        "get_workload_runtime",
        lambda deployment, is_full: _runtime_state(),
    )

    state = await http_observer._get_workload_state(
        k8s=fake_k8s,
        scenario=_scenario(),
    )

    assert state.ready_replicas == 3
    fake_k8s.read_namespaced_deployment.assert_awaited_once()


@pytest.mark.asyncio
async def test_measure_endpoint_latency_collects_state_and_emits_metric(monkeypatch):
    telemetry = TelemetrySpy()
    monkeypatch.setattr(
        http_observer,
        "_collect_interval_requests",
        AsyncMock(return_value=["task-a", "task-b"]),
    )
    monkeypatch.setattr(
        http_observer,
        "_get_workload_state",
        AsyncMock(return_value=_runtime_state()),
    )
    monkeypatch.setattr(
        http_observer,
        "_split_request_results",
        lambda completed_tasks: ([_timed_response()], deque(), 0),
    )
    monkeypatch.setattr(
        http_observer.h, "utc_now_iso", lambda: "2026-07-07T12:00:00+00:00"
    )
    monkeypatch.setattr(http_observer, "KubernetesClient", lambda: SimpleNamespace())

    async with scenario_context(
        telemetry=telemetry,
        scenario=_scenario(),
    ):
        await http_observer.measure_endpoint_latency(
            endpoint="https://example.com/health",
            request_timeout_seconds=2,
            requests_per_interval=2,
        )

    assert len(telemetry.metrics) == 1
    metric = telemetry.metrics[0]
    assert metric.measurement["request_count"] == 1
    assert metric.workload_state.ready_replicas == 3
