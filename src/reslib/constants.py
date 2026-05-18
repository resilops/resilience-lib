from enum import Enum
from typing import Awaitable, Callable, Tuple

POD_RUNNING_STATUS = "Running"
DEPLOYMENT_CONDITION_AVAILABLE = "Available"
DEPLOYMENT_CONDITION_PROGRESSING = "Progressing"
PHASE_EXECUTION_MAX_TIMEOUT: int = 1800  # Upper limit cutoff

DEPLOYMENT_STATUS_RS_AVAILABLE = "NewReplicaSetAvailable"
DEPLOYMENT_STATUS_MIN_RS_AVAILABLE = "MinimumReplicasAvailable"
DEPLOYMENT_STATUS_PROGRESS_DEADLINE = "ProgressDeadlineExceeded"

HPA_SCALEUP_TASK_BUFFER_TIME: int = 10

POD_WAITING_REASONS_OK: Tuple = ("PodInitializing", "ContainerCreating")

POD_TERMINATED_REASONS_OK: Tuple = ("Completed", None)

CONTAINER_CRASH_MONITOR_TASK_NAME: str = "monitor:container:crash"
HPA_SCALE_POD_READY_MONITOR_TASK_NAME: str = "monitor:hpa:scale:pod:ready"
HPA_SCALE_EVENT_MONITOR_TASK_NAME: str = "monitor:hpa:event:scale"
CPU_STRESS_TASK_NAME_PREFIX: str = "action:stress:pod:cpu"
REACHED_DESIRED_REPLICA_TASK_NAME: str = "monitor:replicas:desired"
REPLICAS_RESTORED_TASK_NAME: str = "monitor:replicas:restored"


class HpaMetricSourceEnum(str, Enum):
    RESOURCE = "Resource"
    PODS = "Pods"
    OBJECT = "Object"
    EXTERNAL = "External"
    CONTAINER_RESOURCE = "ContainerResource"


class HpaResourceTypeEnum(str, Enum):
    CPU = "cpu"
    MEMORY = "memory"


SUPPORTED_HPA_METRIC_SOURCES = {
    HpaMetricSourceEnum.RESOURCE,
}
SUPPORTED_HPA_RESOURCE_TYPES = {
    HpaResourceTypeEnum.CPU,
}


class K8DeploymentKind(str, Enum):
    DEPLOYMENT = "Deployment"


class QuantitySelectionModeEnum(str, Enum):
    PERCENTAGE = "percentage"
    ABSOLUTE = "absolute"


class EventEnum(str, Enum):
    """
    Event identifiers for Resilience Library (Reslib) phases.
    Each event is emitted at key points during scenario execution.
    """

    # Guardrail events
    GUARDRAIL_STARTED = "res:reslib:event:guardrail:started"
    GUARDRAIL_SUCCESS = "res:reslib:event:guardrail:success"
    GUARDRAIL_FAILED = "res:reslib:event:guardrail:failed"

    # Observer events
    OBSERVER_STARTED = "res:reslib:event:observer:started"
    OBSERVER_STOPPED = "res:reslib:event:observer:stopped"
    OBSERVER_FAILED = "res:reslib:event:observer:failed"

    # Action events
    ACTION_STARTED = "res:reslib:event:action:started"
    ACTION_SUCCESS = "res:reslib:event:action:success"
    ACTION_FAILED = "res:reslib:event:action:failed"

    # Rollback events
    ROLLBACK_STARTED = "res:reslib:event:rollback:started"
    ROLLBACK_SUCCESS = "res:reslib:event:rollback:success"
    ROLLBACK_FAILED = "res:reslib:event:rollback:failed"


class MetricsEnum(str, Enum):
    """
    Standardized metric identifiers used by Resilience Library (Reslib) observers.

    Each enum value represents a specific metric that can be recorded
    during the execution of experiments, guards, actions, or observers. These names
    are intended to be consistent across the system, enabling monitoring, alerting,
    and metric aggregation.
    """

    HTTP = "res:reslib:metric:http"


AsyncFunc = Callable[..., Awaitable[None]]


POD_RECOVERY_SCENARIO_TEMPLATE: str = "pod_recovery"
HPA_CPU_STRESS_SCENARIO_TEMPLATE: str = "hpa_cpu_stress"


class WorkloadStatusEnum(str, Enum):
    """Workload status identifiers."""

    healthy = "healthy"
    degraded = "degraded"
    reconciling = "reconciling"
    unavailable = "unavailable"
