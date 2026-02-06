import asyncio
import logging
import random
from typing import Any, Awaitable, Dict, List, Optional, Tuple

from kubernetes import stream
from kubernetes.client import V1Pod

from reslib.constants import (
    CONTAINER_CRASH_MONITOR_TASK_NAME,
    CPU_STRESS_TASK_NAME_PREFIX,
    HPA_SCALE_MONITOR_TASK_NAME,
    HPA_SCALEUP_TASK_BUFFER_TIME,
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
)
from reslib.core.context import set_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import CPUStressCommandFailed, HpaScaledError
from reslib.k8s.schema import HPAMetricSpec, WorkloadState
from reslib.k8s.utils import (
    calculate_hpa_trigger,
    get_hpa_resource_metric,
    get_workload,
    get_workload_pods,
    raise_on_container_fail,
    raise_on_hpa_scale,
)
from reslib.schemas.hpa import HpaCPUStressArgsTemplate

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
        stream_resp = stream.stream(
            k8s.v1_api.connect_get_namespaced_pod_exec,
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
            raise RuntimeError(stderr)

        logger.info(
            "Completed CPU stress",
            extra={"pod": pod.metadata.name, "container": container_name},
        )
        return stdout, stderr
    except TimeoutError as exc:
        raise CPUStressCommandFailed(
            "CPU stress command timeout",
            context={
                "pod": pod.metadata.name,
                "timeout": timeout,
                "container": container_name,
            },
        ) from exc
    except Exception as exc:
        raise CPUStressCommandFailed(
            "Something went wrong with the command stress-ng",
            context={"container": container_name, "exc": str(exc)},
        ) from exc
    finally:
        if stream_resp is not None:
            logger.info(
                f"Stopping the pod exec stress command for pod: {pod.metadata.name}"
            )
            stream_resp.close()


def select_pods_to_stress(
    k8s: KubernetesClient,
    workload: WorkloadState,
    args: HpaCPUStressArgsTemplate,
) -> Tuple[List[V1Pod], int]:
    """
    Determine which pods to stress and the CPU load percentage needed
    to trigger HPA scale-up.

    Returns:
        Tuple of (selected pods, cpu_percent per pod)
    """
    hpa_metric: HPAMetricSpec = get_hpa_resource_metric(
        hpa=workload.spec.hpa,
        metric_source=HpaMetricSourceEnum.RESOURCE,
        resource_type=HpaResourceTypeEnum.CPU,
    )

    pods_to_stress_count, stress_cpu_percent = calculate_hpa_trigger(
        status=workload.status,
        metric=hpa_metric,
        idle_cpu_pct=args.idle_cpu_pct,
        pod_cpu_stress_threshold_pct=args.pod_cpu_stress_threshold_pct,
    )

    if pods_to_stress_count <= 0:
        return [], stress_cpu_percent

    pods = get_workload_pods(
        k8s=k8s, namespace=args.namespace, workload_spec=workload.spec
    )
    pods_to_stress = random.sample(pods, k=pods_to_stress_count)

    return pods_to_stress, stress_cpu_percent


def _build_stress_tasks(
    k8s: KubernetesClient,
    pods_to_stress: List[V1Pod],
    stress_cpu_percent: int,
    args: HpaCPUStressArgsTemplate,
    workload: WorkloadState,
) -> List[Tuple[Awaitable[Any], str]]:
    """
    Build a list of stress and HPA monitoring coroutines with task names.

    Returns:
        List of tuples: (coroutine, task_name)
    """
    tasks: List[Tuple[Awaitable[Any], str]] = [
        (
            execute_cpu_stress(
                k8s=k8s,
                pod=pod,
                cpu_percent=stress_cpu_percent,
                container_name=args.container_name,
                timeout=args.max_stress_duration_seconds,
            ),
            f"{CPU_STRESS_TASK_NAME_PREFIX}:{pod.metadata.name}",
        )
        for pod in pods_to_stress
    ]

    tasks.extend(
        [
            (
                # Stop when there is a scaling
                watch_until(
                    condition=raise_on_hpa_scale,
                    timeout=args.max_stress_duration_seconds,
                    poll_interval=5,
                    k8s=k8s,
                    namespace=args.namespace,
                    workload=workload,
                ),
                HPA_SCALE_MONITOR_TASK_NAME,
            ),
            (
                # Fail fast in case of any errors
                watch_until(
                    condition=raise_on_container_fail,
                    timeout=args.max_stress_duration_seconds,
                    poll_interval=5,
                    k8s=k8s,
                    workload_spec=workload.spec,
                    namespace=args.namespace,
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
    args = HpaCPUStressArgsTemplate(**kwargs)
    k8s = KubernetesClient()

    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)

    # Select pods and compute stress load
    pods_to_stress, stress_cpu_percent = select_pods_to_stress(
        k8s=k8s, workload=workload, args=args
    )
    if not pods_to_stress:
        logger.info("Workload already exceeds HPA CPU threshold; skipping stress")
        return

    # Build stress + HPA monitoring tasks
    stress_tasks = _build_stress_tasks(
        k8s=k8s,
        pods_to_stress=pods_to_stress,
        stress_cpu_percent=stress_cpu_percent,
        args=args,
        workload=workload,
    )

    try:
        await watch_task_group(
            tasks=stress_tasks,
            timeout=args.max_stress_duration_seconds + HPA_SCALEUP_TASK_BUFFER_TIME,
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=True,
        )
    except HpaScaledError as exc:
        logger.info("HPA scaleup success")
        set_context("stress_context", {"workload": workload, **exc.context})
        return exc.context
