import asyncio

from reslib.schemas.pod import WorkloadStabilityArgs


async def wait_for_workload_stability(**kwargs) -> None:
    """
    Wait for a workload to stabilize before proceeding.

    This function simply pauses execution for the configured `wait_period`.
    Typically used as a rollback or guardrail to ensure system stability
    after an action.

    Expected keyword arguments (`**kwargs`):
        namespace (str): Kubernetes namespace of the workload.
        labels (str): Label selector to identify the workload.
        wait_period (int, optional): Seconds to wait for stability (default 60, min 10).
        event_handler (BaseEventRecorder, optional): Recorder to log metrics/events.

    Example:
        await wait_for_workload_stability(namespace="abc", labels="app=myapp")
    """
    args = WorkloadStabilityArgs(**kwargs)
    await asyncio.sleep(args.wait_for_stability)
