import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any, Awaitable, Dict, List, Optional, Tuple

from kubernetes import stream
from kubernetes.client import V1Pod

from reslib.actions.schemas import PodStressSchema
from reslib.constants import (
    CONTAINER_CRASH_MONITOR_TASK_NAME,
    CPU_STRESS_TASK_NAME_PREFIX,
    HPA_SCALE_EVENT_MONITOR_TASK_NAME,
    HPA_SCALE_POD_READY_MONITOR_TASK_NAME,
    HPA_SCALEUP_TASK_BUFFER_TIME,
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
)
from reslib.core.context import get_context, set_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import CPUStressCommandFailed, HpaScalePodReadyError
from reslib.k8s.schema import HPAMetricSpec, WorkloadState
from reslib.k8s.utils import (
    calculate_hpa_trigger,
    get_hpa_resource_metric,
    get_workload_pods,
    raise_on_container_fail,
    raise_on_pod_ready_after_hpa_scale,
    set_hpa_scale_event_context,
)
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)

STDOUT_CHANNEL = 1
STDERR_CHANNEL = 2


async def execute_cpu_stress(
    *,
    k8s: KubernetesClient,
    pod: V1Pod,
    cpu_percent: int,
    container_name: Optional[str],
    timeout: int,
) -> Tuple[str, str]:
    """
    Apply CPU stress to a pod using `stress-ng`.

    Args:
        k8s: Kubernetes client.
        pod: Target pod.
        cpu_percent: CPU load percentage per CPU.
        container_name: Optional container name.
        timeout: Duration in seconds.

    Returns:
        Tuple of (stdout, stderr) from stress command.

    Raises:
        CPUStressCommandFailed: If stress command fails or outputs stderr.
    """
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload
    command = [
        "stress-ng",
        "--cpu",
        "1",
        "--cpu-load",
        str(cpu_percent),
        "--timeout",
        f"{timeout}s",
    ]

    logger.info(
        "Starting CPU stress",
        extra={
            "pod": pod.metadata.name,
            "namespace": pod.metadata.namespace,
            "container": container_name,
            "cpu_percent": cpu_percent,
            "duration": timeout,
        },
    )

    stream_resp = None
    try:
        stream_api = k8s.new_v1_api()
        stream_resp = await asyncio.to_thread(
            stream.stream,
            stream_api.connect_get_namespaced_pod_exec,
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            container=container_name,
            command=command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False,
        )

        await asyncio.to_thread(stream_resp.run_forever, timeout=timeout)

        stdout = stream_resp.read_channel(STDOUT_CHANNEL)
        stderr = stream_resp.read_channel(STDERR_CHANNEL).strip()

        if stderr:
            raise CPUStressCommandFailed(
                error_code="CPU_STRESS_COMMAND_ERROR",
                message="CPU stress command returned an error output.",
                namespace=namespace,
                workload=workload_name,
                context={
                    "rule": "stress-ng produces no stderr output",
                    "observed": {
                        "pod": pod.metadata.name,
                        "container": container_name,
                        "stderr": stderr,
                    },
                },
                fix_hint=(
                    "Inspect stderr output and ensure container resources "
                    "permit CPU stress execution."
                ),
                retryable=False,
            )

        logger.info(
            "Completed CPU stress",
            extra={"pod": pod.metadata.name, "container": container_name},
        )
        return stdout, stderr
    except TimeoutError as exc:
        raise CPUStressCommandFailed(
            error_code="CPU_STRESS_TIMEOUT",
            message="CPU stress execution exceeded allowed timeout.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "stress command completes within timeout_seconds",
                "observed": {
                    "pod": pod.metadata.name,
                    "namespace": pod.metadata.namespace,
                    "container": container_name,
                    "timeout_seconds": timeout,
                },
            },
            fix_hint=(
                "Increase timeout_seconds or reduce CPU stress intensity "
                "to allow the command to complete."
            ),
            retryable=True,
        ) from exc
    except Exception as exc:
        raise CPUStressCommandFailed(
            error_code="CPU_STRESS_EXECUTION_FAILED",
            message="CPU stress command failed during pod execution.",
            namespace=namespace,
            workload=workload_name,
            context={
                "rule": "stress-ng command executes successfully",
                "observed": {
                    "pod": pod.metadata.name,
                    "namespace": pod.metadata.namespace,
                    "container": container_name,
                    "exception_type": type(exc).__name__,
                    "exception_message": str(exc),
                },
            },
            fix_hint=(
                "Verify that `stress-ng` is installed in the container and "
                "the pod allows exec operations."
            ),
            retryable=True,
        ) from exc
    finally:
        if stream_resp is not None:
            logger.info(
                f"Stopping the pod exec stress command for pod: {pod.metadata.name}"
            )
            stream_resp.close()


async def select_pods_to_stress(
    k8s: KubernetesClient,
) -> Tuple[List[V1Pod], int]:
    """
    Determine which pods to stress and the CPU load percentage needed
    to trigger HPA scale-up.

    Returns:
        Tuple of (selected pods, cpu_percent per pod)
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace

    hpa_metric: HPAMetricSpec = get_hpa_resource_metric(
        hpa=workload.spec.hpa,
        metric_source=HpaMetricSourceEnum.RESOURCE,
        resource_type=HpaResourceTypeEnum.CPU,
    )

    pods_to_stress_count, stress_cpu_percent = calculate_hpa_trigger(
        status=workload.runtime,
        metric=hpa_metric,
        idle_cpu_pct=scenario.template.idle_cpu_pct,
        cpu_stress_threshold_pct=scenario.template.cpu_stress_threshold_pct,
    )

    if pods_to_stress_count <= 0:
        return [], stress_cpu_percent

    pods = await get_workload_pods(
        k8s=k8s, namespace=namespace, workload_spec=workload.spec
    )
    pods_to_stress = random.sample(pods, k=pods_to_stress_count)

    return pods_to_stress, stress_cpu_percent


def _build_stress_tasks(
    k8s: KubernetesClient,
    pods_to_stress: List[V1Pod],
    stress_cpu_percent: int,
    args: PodStressSchema,
) -> List[Tuple[Awaitable[Any], str]]:
    """
    Build a list of stress and HPA monitoring coroutines with task names.

    Returns:
        List of tuples: (coroutine, task_name)
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace

    tasks: List[Tuple[Awaitable[Any], str]] = [
        (
            execute_cpu_stress(
                k8s=k8s,
                pod=pod,
                cpu_percent=stress_cpu_percent,
                container_name=scenario.template.container_name,
                timeout=args.max_stress_duration_seconds,
            ),
            f"{CPU_STRESS_TASK_NAME_PREFIX}:{pod.metadata.name}",
        )
        for pod in pods_to_stress
    ]

    tasks.extend(
        [
            (
                # Set hpa scale event context
                set_hpa_scale_event_context(
                    k8s=k8s,
                    namespace=namespace,
                    workload=workload,
                    not_before=datetime.now(timezone.utc),
                ),
                HPA_SCALE_EVENT_MONITOR_TASK_NAME,
            ),
            (
                # Stop when there is a scaling
                watch_until(
                    condition=raise_on_pod_ready_after_hpa_scale,
                    timeout=args.max_stress_duration_seconds,
                    poll_interval=5,
                    k8s=k8s,
                    namespace=namespace,
                    workload=workload,
                ),
                HPA_SCALE_POD_READY_MONITOR_TASK_NAME,
            ),
            (
                # Fail fast in case of any errors
                watch_until(
                    condition=raise_on_container_fail,
                    timeout=args.max_stress_duration_seconds,
                    poll_interval=10,
                    k8s=k8s,
                    workload_spec=workload.spec,
                    namespace=namespace,
                ),
                CONTAINER_CRASH_MONITOR_TASK_NAME,
            ),
        ]
    )

    return tasks


async def stress_cpu_hpa(**kwargs) -> Optional[Dict]:
    """
    Apply controlled CPU stress to a subset of pods to trigger CPU-based HPA scale-up.

    Workflow:
        1. Discover the workload and its HPA configuration.
        2. Determine how many pods must be stressed and CPU load per pod.
        3. Apply CPU stress concurrently while monitoring HPA scaling.

    Args:
        **kwargs: Parameters defined by `HpaCPUStressArgsTemplate`.

    Raises:
        CPUStressCommandFailed: If stress execution fails on any pod.
        TimeoutError: If HPA does not scale in the expected duration.
    """
    logger.info("Starting CPU HPA stress test")
    args = PodStressSchema(**kwargs)
    k8s = KubernetesClient()

    workload: WorkloadState = get_context("workload")

    # Select pods and compute stress load
    pods_to_stress, stress_cpu_percent = await select_pods_to_stress(k8s=k8s)
    if not pods_to_stress:
        logger.info("Workload already exceeds HPA CPU threshold; skipping stress")
        return {}

    # Build stress + HPA monitoring tasks
    stress_tasks = _build_stress_tasks(
        k8s=k8s,
        pods_to_stress=pods_to_stress,
        stress_cpu_percent=stress_cpu_percent,
        args=args,
    )

    try:
        await watch_task_group(
            tasks=stress_tasks,
            timeout=args.max_stress_duration_seconds + HPA_SCALEUP_TASK_BUFFER_TIME,
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=True,
        )
    except HpaScalePodReadyError as exc:
        logger.info("HPA scaleup success")
        observed = exc.context.get("observed")
        scale_event = get_context("hpa_scale_event", raise_error=False)
        context = {**observed, **scale_event}
        set_context("stress_context", {"workload": workload, **context})
        return {
            "result": "hpa_scale_up_detected",
            "reason": "CPU stress triggered HPA scale-up",
            "observed": context,
        }
