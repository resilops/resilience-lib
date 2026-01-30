import logging
from typing import Any, Dict, Optional

from kubernetes import config as k8sconfig

from reslib import helpers as h
from reslib.config import config
from reslib.constants import AsyncFunc, EventEnum
from reslib.core.context import ObserverContext
from reslib.logging import setup_logging
from reslib.runtime.phases import ExecutionPhase
from reslib.runtime.resolve import resolve
from reslib.schemas.scenario import (
    ActionSpec,
    BaseOptionalSpec,
    BaseSpec,
    GuardRailSpec,
    ObserverSpec,
    RollbackSpec,
)
from reslib.schemas.telemetry import EventPayload

logger = logging.getLogger(__name__)


def _lib_setup() -> None:
    """
    Perform initial application setup.

    - Configure logging
    - Load Kubernetes configuration

    Prefers in-cluster configuration when running inside Kubernetes.
    Falls back to local kubeconfig for development.
    """
    setup_logging()

    if config.in_cluster_config:
        logger.info("Loading in-cluster Kubernetes configuration")
        k8sconfig.load_incluster_config()
    else:
        logger.info("Loading local kubeconfig")
        k8sconfig.load_kube_config()


async def _execute_phase(
    spec: BaseSpec | BaseOptionalSpec,
    phase: ExecutionPhase,
    success_event: EventEnum,
    failure_event: EventEnum,
    telemetry: h.BaseTelemetry,
) -> None:
    """
    Execute a single resilience phase and record its outcome.

    This function:
      1. Resolves the async handler for the given phase and spec name
      2. Executes the handler with provided keyword arguments
      3. Records success or failure events
      4. Re-raises exceptions for caller handling

    Args:
        spec: Phase specification containing handler name and arguments.
        phase: Execution phase used to resolve the handler.
        success_event: Event recorded when the phase succeeds.
        failure_event: Event recorded when the phase fails.
        telemetry: Recorder used to log phase execution events and metrics.
    """
    if not spec.name:
        return

    error: Optional[Exception] = None
    try:
        func: AsyncFunc = resolve(phase=phase, name=spec.name)
        await func(**spec.kwargs, telemetry=telemetry)
    except Exception as exc:
        error = exc
        raise exc
    finally:
        event = EventPayload(
            event_name=failure_event if error else success_event,
            phase=phase,
            details=str(error) if error else None,
        )
        telemetry.emit_event(event=event)


async def execute_resilience_scenario(
    *,
    action: Dict[str, Any],
    observer: Dict[str, Any],
    guardrail: Optional[Dict[str, Any]] = None,
    rollback: Optional[Dict[str, Any]] = None,
    telemetry: Optional[h.BaseTelemetry] = None,
) -> None:
    """
    Execute a full resilience scenario.

    Execution order:
      1. Guardrail — validate preconditions (fatal on failure)
      2. Action — execute the primary resilience action under observation
      3. Rollback — restore system state if defined

    The observer context is active during action and rollback execution.
    Action and rollback failures are considered fatal and will be re-raised.

    Args:
        action: Definition of the resilience action to execute.
        observer: Definition of the observer used to monitor behavior.
        guardrail: Optional definition of precondition checks.
        rollback: Optional definition of rollback behavior.
        telemetry: Telemetry recorder used to emit lifecycle and result events/metrics.
    """
    _lib_setup()
    telemetry = telemetry or h.NoopTelemetry()

    action_spec = ActionSpec(**action)
    observer_spec = ObserverSpec(**observer)
    guardrail_spec = GuardRailSpec(**(guardrail or {}))
    rollback_spec = RollbackSpec(**(rollback or {}))

    # 1. Guardrail (fatal on failure)
    await _execute_phase(
        spec=guardrail_spec,
        phase=ExecutionPhase.GUARDRAIL,
        success_event=EventEnum.GUARDRAIL_SUCCESS,
        failure_event=EventEnum.GUARDRAIL_FAILED,
        telemetry=telemetry,
    )

    # 2. Action + Observation + Rollback
    async with ObserverContext(telemetry=telemetry, spec=observer_spec):
        await _execute_phase(
            spec=action_spec,
            phase=ExecutionPhase.ACTION,
            success_event=EventEnum.ACTION_SUCCESS,
            failure_event=EventEnum.ACTION_FAILED,
            telemetry=telemetry,
        )
        await _execute_phase(
            spec=rollback_spec,
            phase=ExecutionPhase.ROLLBACK,
            success_event=EventEnum.ROLLBACK_SUCCESS,
            failure_event=EventEnum.ROLLBACK_FAILED,
            telemetry=telemetry,
        )
