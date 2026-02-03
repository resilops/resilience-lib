import asyncio
import time
from typing import Any, Awaitable, Callable, List, Optional, Tuple


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

    # Raise first exception if requested
    if raise_exception:
        for task in done:
            exc = task.exception()
            if exc:
                raise exc

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
    timeout_exception = timeout_exception or TimeoutError(
        f"watch_until timed out after {timeout}s for condition: {condition.__name__}"
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
