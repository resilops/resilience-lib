import asyncio
import logging
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Dict, Optional

from reslib import helpers as h
from reslib.constants import AsyncFunc, EventEnum
from reslib.exceptions import BaseError, ScenarioContextError
from reslib.runtime.phases import ExecutionPhase
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.telemetry import EventPayload

logger = logging.getLogger(__name__)

__all__ = ("ScenarioContext", "ObserverContext", "get_context", "set_context")


# Context variable holding the library/app context
_scenario_ctx: ContextVar[Dict[str, Any]] = ContextVar("scenario_context")


@asynccontextmanager
async def scenario_context(**data: Any):
    """
    Async context manager for a per-execution context dictionary.

    All passed data (functions, objects, primitives, etc.) will be
    available in the context and can be read/updated anywhere inside
    the block.

    Usage:
        async with lib_context(scenario=..., telemetry=..., other=123) as ctx:
            ctx['phase'] = 'ACTION'
    """
    # copy to avoid accidental mutation of passed dicts
    token = _scenario_ctx.set(data.copy())
    try:
        yield _scenario_ctx.get()
    finally:
        _scenario_ctx.reset(token)


def get_context(key: str) -> Any:
    """
    Get a value from the current context dict by key.

    Args:
        key: The key to look up in the context.

    Returns:
        The value associated with the key, or the default if not found.

    Raises:
        LookupError: If there is no active context.
    """
    try:
        ctx: Dict[str, Any] = _scenario_ctx.get()
    except LookupError:
        raise ScenarioContextError("ScenarioContext is not active")

    if key not in ctx:
        raise ScenarioContextError(f"Context key '{key}' is not set")

    return ctx[key]


def set_context(key: str, value: Any) -> None:
    """Set a value in the current context dict."""
    ctx = _scenario_ctx.get()
    ctx[key] = value


class ObserverContext:
    """
    Async context manager to run an observer for a scenario.

    Lifecycle:
        1. Starts the observer asynchronously.
        2. Waits for warmup period to establish baseline.
        3. Main experiment/action executes.
        4. Observer continues to run for grace period after action.
        5. Observer stops gracefully.

    Example:
        async with ObserverContext(observer_spec):
            await terminate_pods()
    """

    def __init__(self, resolver: Any):
        self.resolver = resolver
        self.scenario: Optional[ResiliencyScenario] = None
        self.telemetry: Optional[h.BaseTelemetry] = None
        self._task: Optional[asyncio.Task] = None

    async def _observer_loop(self):
        """Run the observer function repeatedly at the configured sampling interval."""
        observer_func: AsyncFunc = self.resolver.resolve(
            phase=ExecutionPhase.OBSERVER, name=self.scenario.observer.name
        )
        while True:
            await observer_func(**self.scenario.observer.kwargs)
            await asyncio.sleep(self.scenario.observer.config.sampling_interval_seconds)

    async def start(self) -> None:
        """
        Start the observer task and wait for warmup completion.

        If the observer fails during warmup, the exception is raised
        and the scenario execution is aborted.
        """
        logger.info("Starting observer: %s", self.scenario.observer.name)
        scenario_ctx: ResiliencyScenario = get_context("scenario")
        namespace = scenario_ctx.template.namespace
        workload = scenario_ctx.template.workload
        self.telemetry.emit_event(
            event=EventPayload(
                event_name=EventEnum.OBSERVER_STARTED,
                namespace=namespace,
                workload=workload,
                phase=ExecutionPhase.OBSERVER,
                function=self.scenario.observer.name,
            )
        )

        self._task = asyncio.create_task(
            self._observer_loop(),
            name=f"observer:{self.scenario.observer.name}",
        )

        # Allow observer to establish baseline
        if self.scenario.observer.config.warmup_period_seconds > 0:
            logger.info(
                "Observer %s warming up for %s seconds",
                self.scenario.observer.name,
                self.scenario.observer.config.warmup_period_seconds,
            )
            await asyncio.sleep(self.scenario.observer.config.warmup_period_seconds)

        # Fail fast if observer, if there are any errors during warmup
        if self._task.done():
            exc = self._task.exception()
            if exc:
                self.telemetry.emit_event(
                    event=EventPayload(
                        event_name=EventEnum.OBSERVER_FAILED,
                        namespace=namespace,
                        workload=workload,
                        phase=ExecutionPhase.OBSERVER,
                        function=self.scenario.observer.name,
                        data=exc.to_dict() if isinstance(exc, BaseError) else str(exc),
                        error=exc.__class__.__name__,
                    )
                )
                raise exc

    async def stop(self) -> None:
        """Stop the observer, respecting the grace period."""
        if not self._task:
            return

        if self.scenario.observer.config.grace_period_seconds > 0:
            logger.info(
                "Observer %s continuing for grace period: %s seconds",
                self.scenario.observer.name,
                self.scenario.observer.config.grace_period_seconds,
            )
            await asyncio.sleep(self.scenario.observer.config.grace_period_seconds)

        logger.info("Stopping observer: %s", self.scenario.observer.name)
        self._task.cancel()

        try:
            await self._task
        except asyncio.CancelledError:
            logger.debug("Observer task %s cancelled", self.scenario.observer.name)
        finally:
            scenario_ctx: ResiliencyScenario = get_context("scenario")
            namespace = scenario_ctx.template.namespace
            workload = scenario_ctx.template.workload
            self.telemetry.emit_event(
                event=EventPayload(
                    event_name=EventEnum.OBSERVER_STOPPED,
                    namespace=namespace,
                    workload=workload,
                    phase=ExecutionPhase.OBSERVER,
                    function=self.scenario.observer.name,
                )
            )

    async def __aenter__(self) -> "ObserverContext":
        """Enter the async context and start the observer."""
        self.telemetry = get_context("telemetry")
        self.scenario = get_context("scenario")
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Exit the async context and stop the observer gracefully."""
        await self.stop()
        self.scenario, self.telemetry, self._task = None, None, None


ScenarioContext = scenario_context
