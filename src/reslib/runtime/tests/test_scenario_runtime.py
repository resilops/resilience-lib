from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from reslib.constants import EventEnum
from reslib.core.context import scenario_context
from reslib.exceptions import BaseError
from reslib.runtime import scenario as runtime_scenario
from reslib.runtime.phases import ExecutionPhase


class TelemetrySpy:
    def __init__(self):
        self.events = []

    def emit_event(self, *, event):
        self.events.append(event)

    def emit_metrics(self, *, metrics):
        raise AssertionError("metrics not expected")


def _step(step_type, name, **params):
    return SimpleNamespace(type=step_type, name=name, params=params)


def _scenario(steps):
    return SimpleNamespace(
        template=SimpleNamespace(namespace="payments", workload="checkout"),
        steps=steps,
        observer=SimpleNamespace(
            name="measure_endpoint_latency",
            params={"endpoint": "https://example.com/health"},
            config=SimpleNamespace(
                sampling_interval_seconds=0,
                warmup_period_seconds=0,
                grace_period_seconds=0,
            ),
        ),
    )


def test_lib_setup_uses_local_or_incluster_config(monkeypatch):
    setup_logging = []
    monkeypatch.setattr(
        runtime_scenario, "setup_logging", lambda: setup_logging.append(True)
    )
    monkeypatch.setattr(
        runtime_scenario.k8sconfig,
        "load_kube_config",
        lambda: setup_logging.append("local"),
    )
    monkeypatch.setattr(
        runtime_scenario.k8sconfig,
        "load_incluster_config",
        lambda: setup_logging.append("cluster"),
    )

    monkeypatch.setattr(runtime_scenario.config, "in_cluster_config", False)
    runtime_scenario._lib_setup()

    monkeypatch.setattr(runtime_scenario.config, "in_cluster_config", True)
    runtime_scenario._lib_setup()

    assert setup_logging == [True, "local", True, "cluster"]


@pytest.mark.asyncio
async def test_execute_phase_emits_start_and_success_events(monkeypatch):
    telemetry = TelemetrySpy()
    calls = []

    async def action(**kwargs):
        calls.append(kwargs)
        return {"done": True}

    monkeypatch.setattr(
        runtime_scenario.resolver,
        "resolve",
        lambda *, phase, name: action,
    )

    async with scenario_context(
        telemetry=telemetry,
        scenario=_scenario(
            [
                _step(ExecutionPhase.GUARDRAIL, "guard"),
                _step(ExecutionPhase.ACTION, "act", value=1),
            ]
        ),
    ):
        await runtime_scenario._execute_phase(
            phase=ExecutionPhase.ACTION,
            start_event=EventEnum.ACTION_STARTED,
            success_event=EventEnum.ACTION_SUCCESS,
            failure_event=EventEnum.ACTION_FAILED,
        )

    assert calls == [{"value": 1}]
    assert [event.event_name for event in telemetry.events] == [
        EventEnum.ACTION_STARTED,
        EventEnum.ACTION_SUCCESS,
    ]


@pytest.mark.asyncio
async def test_execute_phase_emits_failure_for_base_error(monkeypatch):
    telemetry = TelemetrySpy()
    failure = BaseError("ACTION_FAILED", "boom", "retry")

    async def action(**kwargs):
        raise failure

    monkeypatch.setattr(runtime_scenario.resolver, "resolve", lambda **_: action)

    async with scenario_context(
        telemetry=telemetry,
        scenario=_scenario([_step(ExecutionPhase.ACTION, "act")]),
    ):
        with pytest.raises(BaseError, match="boom"):
            await runtime_scenario._execute_phase(
                phase=ExecutionPhase.ACTION,
                start_event=EventEnum.ACTION_STARTED,
                success_event=EventEnum.ACTION_SUCCESS,
                failure_event=EventEnum.ACTION_FAILED,
            )

    assert telemetry.events[-1].event_name is EventEnum.ACTION_FAILED
    assert telemetry.events[-1].data == failure.to_dict()


@pytest.mark.asyncio
async def test_execute_phase_emits_failure_for_timeout_and_generic_error(monkeypatch):
    telemetry = TelemetrySpy()

    async def timeout_action(**kwargs):
        raise TimeoutError()

    monkeypatch.setattr(
        runtime_scenario.resolver, "resolve", lambda **_: timeout_action
    )
    async with scenario_context(
        telemetry=telemetry,
        scenario=_scenario([_step(ExecutionPhase.ACTION, "act")]),
    ):
        with pytest.raises(TimeoutError):
            await runtime_scenario._execute_phase(
                phase=ExecutionPhase.ACTION,
                start_event=EventEnum.ACTION_STARTED,
                success_event=EventEnum.ACTION_SUCCESS,
                failure_event=EventEnum.ACTION_FAILED,
            )

    assert telemetry.events[-1].data["error_code"] == "PHASE_TIMEOUT"

    telemetry = TelemetrySpy()

    async def generic_action(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        runtime_scenario.resolver, "resolve", lambda **_: generic_action
    )
    async with scenario_context(
        telemetry=telemetry,
        scenario=_scenario([_step(ExecutionPhase.ACTION, "act")]),
    ):
        with pytest.raises(RuntimeError, match="boom"):
            await runtime_scenario._execute_phase(
                phase=ExecutionPhase.ACTION,
                start_event=EventEnum.ACTION_STARTED,
                success_event=EventEnum.ACTION_SUCCESS,
                failure_event=EventEnum.ACTION_FAILED,
            )

    assert telemetry.events[-1].error == "RuntimeError"
    assert telemetry.events[-1].data == {
        "error_code": "UNKNOWN_ERROR",
        "message": "boom",
    }


@pytest.mark.asyncio
async def test_execute_resilience_scenario_runs_guardrail_action_and_rollback(
    monkeypatch,
):
    phases = []
    observer_events = []

    class ObserverStub:
        def __init__(self, resolver):
            self.resolver = resolver

        async def __aenter__(self):
            observer_events.append("enter")
            return self

        async def __aexit__(self, exc_type, exc, tb):
            observer_events.append("exit")

    async def fake_execute_phase(*, phase, **kwargs):
        phases.append(phase)

    monkeypatch.setattr(runtime_scenario, "_lib_setup", lambda: phases.append("setup"))
    monkeypatch.setattr(
        runtime_scenario,
        "get_workload",
        AsyncMock(return_value=SimpleNamespace(spec=SimpleNamespace(name="checkout"))),
    )
    monkeypatch.setattr(runtime_scenario, "_execute_phase", fake_execute_phase)
    monkeypatch.setattr(runtime_scenario, "ObserverContext", ObserverStub)

    await runtime_scenario.execute_resilience_scenario(
        scenario=_scenario(
            [
                _step(ExecutionPhase.GUARDRAIL, "guard"),
                _step(ExecutionPhase.ACTION, "act"),
                _step(ExecutionPhase.ROLLBACK, "rollback"),
            ]
        ),
        telemetry=TelemetrySpy(),
    )

    assert phases == [
        "setup",
        ExecutionPhase.GUARDRAIL,
        ExecutionPhase.ACTION,
        ExecutionPhase.ROLLBACK,
    ]
    assert observer_events == ["enter", "exit"]
