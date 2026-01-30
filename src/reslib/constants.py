from enum import Enum
from typing import Awaitable, Callable

POD_RUNNING_STATUS = "Running"
DEPLOYMENT_CONDITION_AVAILABLE = "Available"
DEPLOYMENT_CONDITION_PROGRESSING = "Progressing"

DEPLOYMENT_STATUS_RS_AVAILABLE = "NewReplicaSetAvailable"
DEPLOYMENT_STATUS_MIN_RS_AVAILABLE = "MinimumReplicasAvailable"
DEPLOYMENT_STATUS_PROGRESS_DEADLINE = "ProgressDeadlineExceeded"


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
    GUARDRAIL_STARTED: str = "res:reslib:event:guardrail:started"
    GUARDRAIL_SUCCESS: str = "res:reslib:event:guardrail:success"
    GUARDRAIL_FAILED: str = "res:reslib:event:guardrail:failed"

    # Observer events
    OBSERVER_STARTED: str = "res:reslib:event:observer:started"
    OBSERVER_STOPPED: str = "res:reslib:event:observer:stopped"
    OBSERVER_FAILED: str = "res:reslib:event:observer:failed"

    # Action events
    ACTION_STARTED: str = "res:reslib:event:action:started"
    ACTION_SUCCESS: str = "res:reslib:event:action:success"
    ACTION_FAILED: str = "res:reslib:event:action:failed"

    # Rollback events
    ROLLBACK_STARTED: str = "res:reslib:event:rollback:started"
    ROLLBACK_SUCCESS: str = "res:reslib:event:rollback:success"
    ROLLBACK_FAILED: str = "res:reslib:event:rollback:failed"


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
