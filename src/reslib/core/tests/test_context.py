import asyncio
from types import SimpleNamespace

import pytest

from reslib.constants import EventEnum
from reslib.core.context import (
    ObserverContext,
    get_context,
    scenario_context,
    set_context,
)
from reslib.exceptions import BaseError, ScenarioContextError
from reslib.runtime.phases import ExecutionPhase


class TelemetrySpy:
    def __init__(self) -> None:
        self.events = []

    def emit_event(self, *, event) -> None:
        self.events.append(event)

    def emit_metrics(self, *, metrics) -> None:
        raise AssertionError("metrics should not be emitted in context tests")


class ResolverStub:
    def __init__(self, observer_func):
        self.observer_func = observer_func
        self.calls = []

    def resolve(self, *, phase, name):
        self.calls.append((phase, name))
        return self.observer_func


def _scenario(*, warmup: float = 0, grace: float = 0, params=None):
    return SimpleNamespace(
        observer=SimpleNamespace(
            name="measure_endpoint_latency",
            params=params or {"endpoint": "https://example.com/health"},
            config=SimpleNamespace(
                sampling_interval_seconds=0,
                warmup_period_seconds=warmup,
                grace_period_seconds=grace,
            ),
        )
    )


@pytest.mark.asyncio
async def test_scenario_context_exposes_and_updates_values():
    async with scenario_context(workload="checkout", attempts=1):
        assert get_context("workload") == "checkout"

        set_context("attempts", 2)

        assert get_context("attempts") == 2


@pytest.mark.asyncio
async def test_get_context_raises_without_active_context():
    with pytest.raises(ScenarioContextError, match="not active"):
        get_context("workload")


@pytest.mark.asyncio
async def test_get_context_raises_for_missing_key():
    async with scenario_context():
        with pytest.raises(ScenarioContextError, match="missing_key"):
            get_context("missing_key")


@pytest.mark.asyncio
async def test_get_context_returns_default_when_requested():
    async with scenario_context():
        assert (
            get_context("missing_key", default="fallback", raise_error=False)
            == "fallback"
        )


@pytest.mark.asyncio
async def test_scenario_context_restores_outer_context_values():
    async with scenario_context(workload="outer"):
        async with scenario_context(workload="inner"):
            assert get_context("workload") == "inner"

        assert get_context("workload") == "outer"


@pytest.mark.asyncio
async def test_observer_context_runs_start_and_stop_lifecycle():
    telemetry = TelemetrySpy()
    observer_calls = []

    async def observer_func(**kwargs):
        observer_calls.append(kwargs)
        await asyncio.sleep(0)

    resolver = ResolverStub(observer_func)

    async with scenario_context(telemetry=telemetry, scenario=_scenario()):
        async with ObserverContext(resolver):
            await asyncio.sleep(0)

    assert resolver.calls == [(ExecutionPhase.OBSERVER, "measure_endpoint_latency")]
    assert observer_calls
    assert observer_calls[0] == {"endpoint": "https://example.com/health"}
    assert [event.event_name for event in telemetry.events] == [
        EventEnum.OBSERVER_STARTED,
        EventEnum.OBSERVER_STOPPED,
    ]


@pytest.mark.asyncio
async def test_observer_context_emits_failure_event_for_base_error():
    telemetry = TelemetrySpy()
    scenario = _scenario(warmup=0.001)
    expected_error = BaseError(
        error_code="OBSERVER_FAILED",
        message="probe failed",
        fix_hint="check endpoint",
    )

    async def failing_observer(**kwargs):
        raise expected_error

    resolver = ResolverStub(failing_observer)
    context = ObserverContext(resolver)
    context.telemetry = telemetry
    context.scenario = scenario

    with pytest.raises(BaseError, match="probe failed"):
        await context.start()

    assert [event.event_name for event in telemetry.events] == [
        EventEnum.OBSERVER_STARTED,
        EventEnum.OBSERVER_FAILED,
    ]
    assert telemetry.events[-1].data == expected_error.to_dict()
    assert telemetry.events[-1].error == "BaseError"


@pytest.mark.asyncio
async def test_observer_context_emits_failure_event_for_generic_error():
    telemetry = TelemetrySpy()
    scenario = _scenario(warmup=0.001)

    async def failing_observer(**kwargs):
        raise RuntimeError("boom")

    resolver = ResolverStub(failing_observer)
    context = ObserverContext(resolver)
    context.telemetry = telemetry
    context.scenario = scenario

    with pytest.raises(RuntimeError, match="boom"):
        await context.start()

    assert telemetry.events[-1].event_name is EventEnum.OBSERVER_FAILED
    assert telemetry.events[-1].data == {"": "UNKNOWN_ERROR", "message": "boom"}
    assert telemetry.events[-1].error == "RuntimeError"


@pytest.mark.asyncio
async def test_observer_context_stop_without_task_is_noop():
    telemetry = TelemetrySpy()
    context = ObserverContext(ResolverStub(lambda **kwargs: None))
    context.telemetry = telemetry
    context.scenario = _scenario()

    await context.stop()

    assert telemetry.events == []
