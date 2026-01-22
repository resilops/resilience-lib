import asyncio
import logging
from typing import Optional
from reslib.constants import AsyncFunc
from reslib.schemas.scenario import ObserverSpec
from reslib.runtime.resolve import resolve
from reslib.runtime.phases import ExecutionPhase

logger = logging.getLogger(__name__)


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

    def __init__(self, spec: ObserverSpec):
        self.spec = spec
        self._task: Optional[asyncio.Task] = None

    async def _observer_loop(self):
        """Run the observer function repeatedly at the configured sampling interval."""
        observer_func: AsyncFunc = resolve(
            phase=ExecutionPhase.OBSERVER, name=self.spec.name
        )
        while True:
            await observer_func(**self.spec.kwargs)
            await asyncio.sleep(self.spec.sampling_interval)

    async def start(self) -> None:
        """
        Start the observer task and wait for warmup completion.

        If the observer fails during warmup, the exception is raised
        and the scenario execution is aborted.
        """
        logger.info("Starting observer: %s", self.spec.name)
        self._task = asyncio.create_task(
            self._observer_loop(), name=f"observer:{self.spec.name}",
        )

        # Allow observer to establish baseline
        if self.spec.warmup_period > 0:
            logger.info(
                "Observer %s warming up for %s seconds",
                self.spec.name,
                self.spec.warmup_period,
            )
            await asyncio.sleep(self.spec.warmup_period)

        # Fail fast if observer, if there are any errors during warmup
        if self._task.done():
            exc = self._task.exception()
            if exc:
                raise exc

    async def stop(self) -> None:
        """Stop the observer, respecting the grace period."""
        if not self._task:
            return

        if self.spec.grace_period > 0:
            logger.info(
                "Observer %s continuing for grace period: %s seconds",
                self.spec.name,
                self.spec.grace_period
            )
            await asyncio.sleep(self.spec.grace_period)

        logger.info("Stopping observer: %s", self.spec.name)
        self._task.cancel()

        try:
            await self._task
        except asyncio.CancelledError:
            logger.debug("Observer task %s cancelled", self.spec.name)
        finally:
            self._task = None

    async def __aenter__(self) -> "ObserverContext":
        """Enter the async context and start the observer."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Exit the async context and stop the observer gracefully."""
        await self.stop()
