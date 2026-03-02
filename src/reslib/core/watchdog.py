import asyncio
import time
from typing import Any, Awaitable, Callable, List, Optional, Tuple

from reslib.exceptions import TaskGroupTimeoutError, TaskTimeoutError


async def watch_task_group(
    tasks: List[Tuple[Awaitable[Any], str]],
    timeout: float = 30.0,
    return_when: str = asyncio.FIRST_EXCEPTION,
    raise_exception: bool = True,
) -> List[asyncio.Task[Any]]:
    """
    Run multiple coroutines concurrently and monitor their completion.

    Features:
        - Optionally raise on first exception.
        - Optionally raise if tasks exceed timeout.
        - Cancels pending tasks after timeout or first exception.

    Args:
        tasks: List of coroutines to monitor.
        timeout: Maximum seconds to wait for tasks to complete.
        return_when: Determines when asyncio.wait returns:
            - asyncio.ALL_COMPLETED
            - asyncio.FIRST_COMPLETED
            - asyncio.FIRST_EXCEPTION
        raise_exception: If True, raises the first exception encountered.

    Returns:
        List of asyncio.Task objects corresponding to the coroutines.

    Raises:
        TimeoutError: If tasks did not complete within the timeout.
        Exception: If any task raised an exception and raise_exception is True.
    """
    if not tasks:
        return []

    # Wrap coroutines into asyncio.Task objects
    tasks: List[asyncio.Task[Any]] = [
        asyncio.create_task(coro, name=name) for coro, name in tasks
    ]

    # Wait for tasks based on return_when and timeout
    done, pending = await asyncio.wait(tasks, timeout=timeout, return_when=return_when)

    # Cancel all pending tasks
    for task in pending:
        task.cancel()

    await asyncio.gather(*pending, return_exceptions=True)

    # Raise first exception if requested
    if raise_exception:
        for task in done:
            exc = task.exception()
            if exc:
                raise exc

    if pending:
        TaskGroupTimeoutError(
            error_code="TASK_GROUP_TIMEOUT",
            message="Task group execution exceeded allowed timeout.",
            context={
                "rule": "all required tasks complete before timeout",
                "observed": {
                    "timeout_seconds": timeout,
                    "completed_tasks": [t.get_name() for t in done],
                    "pending_tasks": [t.get_name() for t in pending],
                },
            },
            fix_hint=("Increase timeout or investigate long-running or blocked tasks."),
            retryable=False,
        )

    return list(done)


async def watch_until(
    *,
    condition: Callable[..., Any],
    timeout: float,
    poll_interval: float = 1.0,
    timeout_exception: Optional[Exception] = None,
    **kwargs,
) -> Any:
    """
    Repeatedly evaluate a condition until it returns a truthy value or timeout occurs.

    Supports both synchronous and asynchronous callables.

    Args:
        condition: A callable (sync or async) that returns a truthy/falsy value.
        timeout: Maximum time in seconds to wait.
        poll_interval: How often to poll the condition (seconds).
        timeout_exception: Exception to raise on timeout (default TimeoutError).
        **kwargs: Keyword arguments to pass to `condition`.

    Returns:
        The value returned by the condition when truthy.

    Raises:
        timeout_exception if timeout occurs, default is TimeoutError.
    """
    deadline = time.monotonic() + timeout
    timeout_exception = timeout_exception or TaskTimeoutError(
        error_code="WATCH_CONDITION_TIMEOUT",
        message="Condition was not satisfied within the allowed timeout.",
        context={
            "rule": "condition evaluates to truthy before timeout",
            "inputs": {
                "condition": repr(condition),
                "timeout_seconds": timeout,
                "poll_interval_seconds": poll_interval,
            },
        },
        fix_hint=(
            "Increase timeout, reduce system load, or verify that the "
            "observed system state can reach the expected condition."
        ),
        retryable=False,
    )

    while True:
        result = condition(**kwargs)
        if asyncio.iscoroutine(result) or isinstance(result, Awaitable):
            result = await result

        if result:
            return result

        if time.monotonic() >= deadline:
            raise timeout_exception

        await asyncio.sleep(poll_interval)
