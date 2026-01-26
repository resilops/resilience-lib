import logging
from typing import Any, Dict, Optional

from kubernetes import config as k8sconfig

from reslib import helpers as h
from reslib.config import config
from reslib.constants import AsyncFunc, ReslibEventEnum
from reslib.core.context import ObserverContext
from reslib.exceptions import PhaseExecutionFailed
from reslib.logging import setup_logging
from reslib.runtime.phases import ExecutionPhase
from reslib.runtime.resolve import resolve
from reslib.schemas.event import ResLibEventPayload
from reslib.schemas.scenario import (
    ActionSpec,
    BaseOptionalSpec,
    BaseSpec,
    GuardRailSpec,
    ObserverSpec,
    RollbackSpec,
)

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
    event_recorder: h.BaseEventRecorder,
    success_event: ReslibEventEnum,
    failure_event: ReslibEventEnum,
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
        event_recorder: Recorder used to log phase execution events.
        success_event: Event recorded when the phase succeeds.
        failure_event: Event recorded when the phase fails.
    """
    if not spec.name:
        return

    logger.info(f"Executing phase: {phase.name}")

    try:
        func: AsyncFunc = resolve(phase=phase, name=spec.name)
        await func(**spec.kwargs, event_recorder=event_recorder)
        event_recorder.record(
            event=ResLibEventPayload(event_name=success_event, phase=phase)
        )
    except Exception as exc:
        event_recorder.record(
            event=ResLibEventPayload(
                event_name=failure_event, phase=phase, is_error=True, error_msg=str(exc)
            )
        )
        raise PhaseExecutionFailed(f"Error executing phase: {phase.name}") from exc


async def execute_resilience_scenario(
    *,
    action: Dict[str, Any],
    observer: Dict[str, Any],
    guardrail: Optional[Dict[str, Any]] = None,
    rollback: Optional[Dict[str, Any]] = None,
    event_recorder: Optional[h.BaseEventRecorder] = None,
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
        event_recorder: Recorder used to emit lifecycle and result events.
    """
    _lib_setup()
    event_recorder = event_recorder or h.NoopEventRecorder()

    action_spec = ActionSpec(**action)
    observer_spec = ObserverSpec(**observer)
    guardrail_spec = GuardRailSpec(**(guardrail or {}))
    rollback_spec = RollbackSpec(**(rollback or {}))

    # 1. Guardrail (fatal on failure)
    await _execute_phase(
        spec=guardrail_spec,
        phase=ExecutionPhase.GUARDRAIL,
        event_recorder=event_recorder,
        success_event=ReslibEventEnum.GUARDRAIL_SUCCESS,
        failure_event=ReslibEventEnum.GUARDRAIL_FAILED,
    )

    # 2. Action + Observation + Rollback
    async with ObserverContext(observer_spec):
        await _execute_phase(
            spec=action_spec,
            phase=ExecutionPhase.ACTION,
            event_recorder=event_recorder,
            success_event=ReslibEventEnum.ACTION_SUCCESS,
            failure_event=ReslibEventEnum.ACTION_FAILED,
        )

        await _execute_phase(
            spec=rollback_spec,
            phase=ExecutionPhase.ROLLBACK,
            event_recorder=event_recorder,
            success_event=ReslibEventEnum.ROLLBACK_SUCCESS,
            failure_event=ReslibEventEnum.ROLLBACK_FAILED,
        )
