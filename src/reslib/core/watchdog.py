import asyncio
from typing import Any, Coroutine, List


async def monitor_tasks(
    watch_tasks: List[Coroutine[Any, Any, Any]],
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
        watch_tasks: List of coroutines to monitor.
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
    if not watch_tasks:
        return []

    # Wrap coroutines into asyncio.Task objects
    tasks: List[asyncio.Task[Any]] = [asyncio.create_task(coro) for coro in watch_tasks]

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

    # Raise timeout if there are still pending tasks
    if pending:
        raise TimeoutError(
            f"{len(pending)} task(s) exceeded the max timeout of {timeout} seconds"
        )

    return tasks
