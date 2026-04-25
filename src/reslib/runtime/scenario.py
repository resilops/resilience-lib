import logging
from typing import Optional

from kubernetes import config as k8sconfig

from reslib import helpers as h
from reslib.config import config
from reslib.constants import PHASE_EXECUTION_MAX_TIMEOUT, AsyncFunc, EventEnum
from reslib.core.context import ObserverContext, ScenarioContext, get_context
from reslib.exceptions import BaseError
from reslib.k8s.schema import WorkloadState
from reslib.k8s.workloads import get_workload
from reslib.logging import setup_logging
from reslib.runtime import resolve as resolver
from reslib.runtime.phases import ExecutionPhase
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
    phase: ExecutionPhase,
    start_event: EventEnum,
    success_event: EventEnum,
    failure_event: EventEnum,
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
        phase: The phase to execute (guardrail, action, rollback).
        start_event: Event recorded on execution phase start.
        success_event: Event recorded on successful execution.
        failure_event: Event recorded on failure.
    """
    telemetry: h.BaseTelemetry = get_context("telemetry")
    scenario: ResiliencyScenario = get_context("scenario")
    for step in filter(lambda s: s.name and s.type == phase, scenario.steps):
        telemetry.emit_event(
            event=EventPayload(
                event_name=start_event,
                phase=phase,
                function=step.name,
            )
        )
        try:
            # async with asyncio.timeout(PHASE_EXECUTION_MAX_TIMEOUT):
            func: AsyncFunc = resolver.resolve(phase=step.type, name=step.name)
            result = await func(**step.params)

            telemetry.emit_event(
                event=EventPayload(
                    event_name=success_event,
                    function=step.name,
                    phase=phase,
                    data=result,
                )
            )
        except BaseError as exc:
            telemetry.emit_event(
                event=EventPayload(
                    event_name=failure_event,
                    phase=phase,
                    function=step.name,
                    error=exc.__class__.__name__,
                    data=exc.to_dict(),
                )
            )
            raise
        except TimeoutError:
            telemetry.emit_event(
                event=EventPayload(
                    event_name=failure_event,
                    phase=phase,
                    function=step.name,
                    error="TimeoutError",
                    data={"timeout_seconds": PHASE_EXECUTION_MAX_TIMEOUT},
                )
            )
            raise
        except Exception as exc:
            telemetry.emit_event(
                event=EventPayload(
                    event_name=failure_event,
                    phase=phase,
                    error=exc.__class__.__name__,
                    function=step.name,
                )
            )
            raise


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

    workload: WorkloadState = await get_workload(
        namespace=scenario.template.namespace,
        name=scenario.template.workload,
    )

    async with ScenarioContext(
        scenario=scenario,
        telemetry=telemetry or h.NoopTelemetry(),
        workload=workload,
    ):
        # 1. Guardrail phase (fatal if validation fails)
        await _execute_phase(
            phase=ExecutionPhase.GUARDRAIL,
            start_event=EventEnum.GUARDRAIL_STARTED,
            success_event=EventEnum.GUARDRAIL_SUCCESS,
            failure_event=EventEnum.GUARDRAIL_FAILED,
        )

        # 2. Action and rollback phases with observer context
        async with ObserverContext(resolver=resolver):
            await _execute_phase(
                phase=ExecutionPhase.ACTION,
                start_event=EventEnum.ACTION_STARTED,
                success_event=EventEnum.ACTION_SUCCESS,
                failure_event=EventEnum.ACTION_FAILED,
            )
            await _execute_phase(
                phase=ExecutionPhase.ROLLBACK,
                start_event=EventEnum.ROLLBACK_STARTED,
                success_event=EventEnum.ROLLBACK_SUCCESS,
                failure_event=EventEnum.ROLLBACK_FAILED,
            )
