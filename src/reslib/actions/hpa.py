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
    HPA_SCALE_POD_READY_MONITOR_TASK_NAME,
    HPA_SCALEUP_TASK_BUFFER_TIME,
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
)
from reslib.core.context import get_context, set_context
from reslib.core.watchdog import watch_task_group, watch_until
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import CPUStressCommandFailed, HpaScalePodReadyError
from reslib.k8s.pods import get_workload_pods, raise_on_container_fail
from reslib.k8s.scaling import (
    HPA_SCALE_UP_EVENT_CONTEXT_KEY,
    calculate_hpa_trigger,
    get_hpa_resource_metric,
    raise_on_scaled_pods_ready,
    wait_for_hpa_scale_up_event,
)
from reslib.k8s.schema import HPAMetricSpec, WorkloadState
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)

STDOUT_CHANNEL = 1
STDERR_CHANNEL = 2


async def run_cpu_stress(
    *,
    k8s: KubernetesClient,
    pod: V1Pod,
    cpu_percent: int,
    container_name: Optional[str],
    timeout: int,
) -> Tuple[str, str]:
    """
    Execute `stress-ng` inside a pod to apply CPU pressure.

    Args:
        k8s: Kubernetes client used to open the exec session.
        pod: Target pod that will run the stress command.
        cpu_percent: CPU load percentage to apply.
        container_name: Optional target container within the pod.
        timeout: Maximum stress duration in seconds.

    Returns:
        Tuple of stdout and stderr collected from the exec session.

    Raises:
        CPUStressCommandFailed: If the stress command writes to stderr.
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
                context={"observed": {"stderr": stderr}},
                retryable=False,
            )

        return stdout, stderr

    finally:
        if stream_resp is not None:
            stream_resp.close()


async def select_pods_to_stress(
    k8s: KubernetesClient,
) -> Tuple[List[V1Pod], int]:
    """
    Select workload pods and compute the CPU load needed to trigger HPA scale-up.

    Args:
        k8s: Kubernetes client used to list workload pods.

    Returns:
        Tuple of selected pods and CPU load percentage per pod.
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
    return random.sample(pods, k=pods_to_stress_count), stress_cpu_percent


def build_hpa_stress_tasks(
    k8s: KubernetesClient,
    pods_to_stress: List[V1Pod],
    stress_cpu_percent: int,
    args: PodStressSchema,
) -> List[Tuple[Awaitable[Any], str]]:
    """
    Build the concurrent stress and monitor tasks for the HPA experiment.

    The returned tasks include:
      - one CPU stress task per selected pod
      - one monitor that stops on scaled pods becoming Ready
      - one fail-fast monitor for container crashes
    """
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace: str = scenario.template.namespace

    tasks = [
        (
            run_cpu_stress(
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
                watch_until(
                    condition=raise_on_scaled_pods_ready,
                    timeout=args.max_stress_duration_seconds,
                    poll_interval=5,
                    k8s=k8s,
                    namespace=namespace,
                    workload=workload,
                ),
                HPA_SCALE_POD_READY_MONITOR_TASK_NAME,
            ),
            (
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
    Apply CPU stress until HPA scale-up is observed and the scaled pods are Ready.

    Workflow:
        1. Parse action arguments and resolve workload context.
        2. Select the workload pods to stress and required CPU load.
        3. Start a passive watcher that records the HPA SuccessfulRescale event.
        4. Run stress tasks and monitors concurrently.
        5. Return the merged scale event and readiness observations on success.

    Args:
        **kwargs: Parameters accepted by `PodStressSchema`.

    Returns:
        Result payload describing the detected HPA scale-up, or `{}` if no stress
        is needed because the workload already meets the trigger condition.

    Raises:
        CPUStressCommandFailed: If CPU stress execution fails.
        HpaScalePodReadyError: Internally used as the success signal when scaled
            pods become Ready.
        Exception: Any monitor failure raised by `watch_task_group`.
    """
    logger.info("Starting CPU HPA stress test")

    args = PodStressSchema(**kwargs)
    k8s = KubernetesClient()
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace

    pods_to_stress, stress_cpu_percent = await select_pods_to_stress(k8s=k8s)
    if not pods_to_stress:
        return {}

    # Capture start time ONCE
    stress_start_time = datetime.now(timezone.utc)

    # Start event watcher in background (passive)
    event_task = asyncio.create_task(
        wait_for_hpa_scale_up_event(
            k8s=k8s,
            namespace=namespace,
            workload=workload,
            not_before=stress_start_time,
        )
    )

    try:
        stress_tasks = build_hpa_stress_tasks(
            k8s=k8s,
            pods_to_stress=pods_to_stress,
            stress_cpu_percent=stress_cpu_percent,
            args=args,
        )

        await watch_task_group(
            tasks=stress_tasks,
            timeout=args.max_stress_duration_seconds + HPA_SCALEUP_TASK_BUFFER_TIME,
            return_when=asyncio.FIRST_EXCEPTION,
            raise_exception=True,
        )

    except HpaScalePodReadyError as exc:
        logger.info("HPA scaleup success")
        observed = exc.context.get("observed", {})
        scale_event = get_context(
            HPA_SCALE_UP_EVENT_CONTEXT_KEY, default={}, raise_error=False
        )
        context = {**observed, **scale_event}
        set_context("stress_context", {"workload": workload, **context})
        return {
            "result": "hpa_scale_up_detected",
            "reason": "CPU stress triggered HPA scale-up",
            "observed": context,
        }

    finally:
        event_task.cancel()
