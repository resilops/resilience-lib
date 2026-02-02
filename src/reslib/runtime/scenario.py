import logging
from typing import Optional

from kubernetes import config as k8sconfig

from reslib import helpers as h
from reslib.config import config
from reslib.constants import AsyncFunc, EventEnum
from reslib.core.context import ObserverContext
from reslib.logging import setup_logging
from reslib.runtime.phases import ExecutionPhase
from reslib.runtime.resolve import resolve
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.telemetry import EventPayload

logger = logging.getLogger(__name__)


def _lib_setup() -> None:
    """
    Initialize the runtime environment.

    - Configures logging according to ResiliencyLib settings.
    - Loads Kubernetes configuration:
        - In-cluster if running inside Kubernetes.
        - Local kubeconfig as fallback (for development/testing).
    """
    setup_logging()

    if config.in_cluster_config:
        logger.info("Loading in-cluster Kubernetes configuration")
        k8sconfig.load_incluster_config()
    else:
        logger.info("Loading local kubeconfig")
        k8sconfig.load_kube_config()


async def _execute_phase(
    *,
    scenario: ResiliencyScenario,
    phase: ExecutionPhase,
    success_event: EventEnum,
    failure_event: EventEnum,
    telemetry: h.BaseTelemetry,
) -> None:
    """
    Execute a single phase of a resilience scenario and emit telemetry events.

    For each step in the scenario:
      1. Filters by step type matching the given phase.
      2. Resolves the async handler using `resolve`.
      3. Executes the handler with merged template and step overrides.
      4. Emits success or failure event to telemetry.
      5. Re-raises exceptions for caller handling.

    Args:
        scenario: Resiliency scenario containing template and steps.
        phase: The phase to execute (guardrail, action, rollback).
        success_event: Event recorded on successful execution.
        failure_event: Event recorded on failure.
        telemetry: Telemetry recorder for emitting lifecycle events.
    """
    for step in scenario.steps:
        if not step.name or step.type != phase:
            continue

        error: Optional[Exception] = None
        try:
            func: AsyncFunc = resolve(phase=step.type, name=step.name)
            await func(**scenario.template, **step.overrides, telemetry=telemetry)
        except Exception as exc:
            error = exc
            logger.exception("Error executing the phase", extra={"phase": phase})
            raise exc
        finally:
            event = EventPayload(
                event_name=failure_event if error else success_event,
                phase=step.type,
                details=str(error) if error else None,
            )
            telemetry.emit_event(event=event)


async def execute_resilience_scenario(
    *,
    scenario: ResiliencyScenario,
    telemetry: Optional[h.BaseTelemetry] = None,
) -> None:
    """
    Execute a full resiliency scenario including guardrail, action, observer,
    and rollback.

    Execution flow:
      1. Guardrail — validates preconditions (fatal if fails)
      2. Action — performs the primary resilience action
      3. Observer — monitors system behavior during action and rollback
      4. Rollback — restores system state if defined

    Failures in any phase are considered fatal and will be re-raised after emitting
    events.

    Args:
        scenario: The resiliency scenario to execute.
        telemetry: Optional telemetry recorder for emitting events and metrics.
                   Defaults to a no-op telemetry implementation if not provided.
    """
    _lib_setup()
    telemetry = telemetry or h.NoopTelemetry()

    # 1. Guardrail phase (fatal if validation fails)
    await _execute_phase(
        scenario=scenario,
        phase=ExecutionPhase.GUARDRAIL,
        success_event=EventEnum.GUARDRAIL_SUCCESS,
        failure_event=EventEnum.GUARDRAIL_FAILED,
        telemetry=telemetry,
    )

    # 2. Action and rollback phases with observer context
    async with ObserverContext(telemetry=telemetry, spec=scenario.observer):
        await _execute_phase(
            scenario=scenario,
            phase=ExecutionPhase.ACTION,
            success_event=EventEnum.ACTION_SUCCESS,
            failure_event=EventEnum.ACTION_FAILED,
            telemetry=telemetry,
        )
        await _execute_phase(
            scenario=scenario,
            phase=ExecutionPhase.ROLLBACK,
            success_event=EventEnum.ROLLBACK_SUCCESS,
            failure_event=EventEnum.ROLLBACK_FAILED,
            telemetry=telemetry,
        )
