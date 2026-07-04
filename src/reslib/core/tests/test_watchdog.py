import asyncio
from types import SimpleNamespace

import pytest

from reslib.core.context import scenario_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.exceptions import TaskGroupTimeoutError, TaskTimeoutError


async def _sleep_and_return(value: str, delay: float = 0.0) -> str:
    await asyncio.sleep(delay)
    return value


@pytest.mark.asyncio
async def test_watch_task_group_returns_completed_tasks():
    done = await watch_task_group(
        tasks=[
            (_sleep_and_return("a"), "task:a"),
            (_sleep_and_return("b"), "task:b"),
        ],
        timeout=1,
        return_when=asyncio.ALL_COMPLETED,
    )

    assert sorted(task.result() for task in done) == ["a", "b"]


@pytest.mark.asyncio
async def test_watch_task_group_raises_timeout_error_with_context():
    scenario = SimpleNamespace(template=SimpleNamespace(workload="checkout-api"))

    async with scenario_context(scenario=scenario):
        with pytest.raises(TaskGroupTimeoutError, match="checkout-api"):
            await watch_task_group(
                tasks=[(_sleep_and_return("late", delay=0.2), "task:late")],
                timeout=0.01,
            )


@pytest.mark.asyncio
async def test_watch_until_supports_sync_conditions():
    scenario = SimpleNamespace(template=SimpleNamespace(workload="checkout-api"))
    attempts = {"count": 0}

    def condition() -> bool:
        attempts["count"] += 1
        return attempts["count"] >= 2

    async with scenario_context(scenario=scenario):
        result = await watch_until(condition=condition, timeout=0.2, poll_interval=0.0)

    assert result is True


@pytest.mark.asyncio
async def test_watch_until_raises_default_timeout_error():
    scenario = SimpleNamespace(template=SimpleNamespace(workload="checkout-api"))

    async def never_ready() -> bool:
        return False

    async with scenario_context(scenario=scenario):
        with pytest.raises(TaskTimeoutError, match="never_ready"):
            await watch_until(
                condition=never_ready,
                timeout=0.01,
                poll_interval=0.0,
            )
