import asyncio
import logging
import random
from typing import Optional

from kubernetes import stream
from kubernetes.client import V1Pod

from reslib.constants import (
    POD_STRESS_DURATION_BUFFER,
    POD_STRESS_TASK_TIMEOUT_BUFFER,
    HpaMetricTypeEnum,
    HpaResourceNameEnum,
)
from reslib.core.watchdog import monitor_tasks
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import CPUStressCommandFailed
from reslib.k8s.schema import HPAMetricSpec, WorkloadState
from reslib.k8s.utils import (
    calculate_cpu_pods_to_stress_count,
    get_deployment_pods,
    get_hpa_resource_metric,
    get_workload,
)
from reslib.schemas.hpa import HpaCPUStressArgsTemplate

logger = logging.getLogger(__name__)

STDOUT_CHANNEL = 1
STDERR_CHANNEL = 2


def _run_stress_command(
    k8s: KubernetesClient,
    pod: V1Pod,
    command: list[str],
    container: Optional[str],
    timeout: int,
) -> tuple[str, str]:
    """
    Blocking function to run a stress command via Kubernetes exec.

    Returns stdout and stderr from the pod.
    Raises CPUStressCommandFailed if exec fails.
    """
    try:
        resp = stream.stream(
            k8s.v1_api.connect_get_namespaced_pod_exec,
            name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            container=container,
            command=command,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False,
        )

        resp.run_forever(timeout=timeout)
        stdout = resp.read_channel(STDOUT_CHANNEL)
        stderr = resp.read_channel(STDERR_CHANNEL).strip()

        if stderr:
            raise CPUStressCommandFailed(
                f"Pod {pod.metadata.name} container={container} "
                f"failed. Reason: {stderr}"
            )

        return stdout, stderr
    except Exception as e:
        raise CPUStressCommandFailed(
            f"Pod {pod.metadata.name} container={container} failed to start stress-ng. "
            f"Reason: {str(e)}"
        ) from e


async def _apply_cpu_stress(
    *,
    k8s: KubernetesClient,
    pod: V1Pod,
    cpu_percent: int,
    duration_seconds: int,
    container: Optional[str] = None,
):
    """
    Apply CPU stress to a specific pod for a limited duration.

    Uses `stress-ng` to generate CPU load inside the target container.

    Args:
        k8s: Kubernetes client instance.
        pod: Target pod object.
        cpu_percent: CPU percentage to load each stressed CPU.
        duration_seconds: Duration for which the stress is applied (seconds).
        container: Optional container name inside the pod.
                   If None, defaults to the first container.

    Notes:
        - Stress automatically stops after `duration_seconds`.
        - Using a sidecar container is recommended to avoid crashing the app container.
    """
    command = [
        "stress-ng",
        "--cpu",
        "1",
        "--cpu-load",
        str(cpu_percent),
        "--timeout",
        f"{duration_seconds}s",
    ]

    logger.info(
        f"Running stress command for pod: {pod.metadata.name} container={container}"
    )
    await asyncio.to_thread(
        _run_stress_command,
        k8s,
        pod,
        command,
        container,
        duration_seconds + POD_STRESS_DURATION_BUFFER,
    )
    logger.info(f"Stress finished for pod: {pod.metadata.name} container={container}")


async def stress_cpu_hpa(**kwargs):
    """
    Apply controlled CPU stress to a subset of pods in a workload to safely trigger
    Horizontal Pod Autoscaler (HPA) scale-up based on CPU utilization.

    Steps:
        1. Discover the target workload and its deployment.
        2. Retrieve HPA metrics and calculate how many pods must be stressed to trigger
           scale-up.
        3. Select pods randomly to distribute stress evenly.
        4. Apply CPU stress concurrently and monitor tasks for early failure.

    Kwargs (via HpaCPUStressArgsTemplate):
        namespace: Kubernetes namespace of the workload.
        workload: Name of the workload (deployment).
        stress_cpu_percent: Target CPU percentage per stressed pod.
        idle_cpu_percent: Baseline CPU usage percentage for pods.
        stress_duration: Duration of the stress in seconds.
        container_name: Optional container name in which to execute stress.

    Raises:
        Exception: Any error occurring during pod execution.
        TimeoutError: If stress tasks do not complete within allowed duration.
    """
    logger.info("Started stressing CPU")
    args = HpaCPUStressArgsTemplate(**kwargs)
    k8s = KubernetesClient()

    # 1. Discover workload and deployment
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)
    deployment = k8s.apps.read_namespaced_deployment(
        name=args.workload, namespace=args.namespace
    )

    # Guardrails ensure HPA exists and CPU resource metric is configured
    hpa_metric: HPAMetricSpec = get_hpa_resource_metric(
        hpa=workload.spec.hpa,
        metric_type=HpaMetricTypeEnum.RESOURCE,
        resource=HpaResourceNameEnum.CPU,
    )

    # 2. Calculate how many pods must be stressed to trigger scale-up
    pods_to_stress_count = calculate_cpu_pods_to_stress_count(
        workload=workload,
        metric=hpa_metric,
        idle_cpu_percent=args.idle_cpu_percent,
        stress_cpu_percent=args.stress_cpu_percent,
    )

    if pods_to_stress_count <= 0:
        return  # Workload already meets HPA threshold, nothing to stress

    # 3. Select pods for CPU stress
    pods = get_deployment_pods(k8s=k8s, namespace=args.namespace, deployment=deployment)
    pods_to_stress = random.sample(pods, k=pods_to_stress_count)

    # 4. Launch stress tasks concurrently
    stress_tasks = [
        _apply_cpu_stress(
            k8s=k8s,
            pod=pod,
            cpu_percent=args.stress_cpu_percent,
            duration_seconds=args.stress_duration,
            container=args.container,  # optional: app or sidecar container
        )
        for pod in pods_to_stress
    ]

    # Monitor tasks and exit early on first failure or timeout
    await monitor_tasks(
        watch_tasks=stress_tasks,
        timeout=args.stress_duration + POD_STRESS_TASK_TIMEOUT_BUFFER,
        return_when=asyncio.FIRST_EXCEPTION,
        raise_exception=True,
    )

    logger.info("Successfully stressed CPU")
