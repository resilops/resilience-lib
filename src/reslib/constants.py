from enum import Enum
from typing import Awaitable, Callable

POD_RUNNING_STATUS = "Running"


class K8DeploymentKind(str, Enum):
    STATELESS = "Stateless"
    STATEFUL = "Stateful"


class QuantitySelectionModeEnum(str, Enum):
    PERCENTAGE = "percentage"
    ABSOLUTE = "absolute"


class ReslibEventEnum(str, Enum):
    """
    Event identifiers for Resilience Library (Reslib) phases.
    Each event is emitted at key points during scenario execution.
    """

    # Guardrail events
    GUARDRAIL_STARTED: str = "res:event:reslib:guardrail:started"
    GUARDRAIL_SUCCESS: str = "res:event:reslib:guardrail:success"
    GUARDRAIL_FAILED: str = "res:event:reslib:guardrail:failed"

    # Observer events
    OBSERVER_STARTED: str = "res:event:reslib:observer:started"
    OBSERVER_STOPPED: str = "res:event:reslib:observer:stopped"
    OBSERVER_FAILED: str = "res:event:reslib:observer:failed"

    # Observer metrics
    OBSERVER_METRICS: str = "res:event:reslib:observer:metrics"

    # Action events
    ACTION_STARTED: str = "res:event:reslib:action:started"
    ACTION_SUCCESS: str = "res:event:reslib:action:success"
    ACTION_FAILED: str = "res:event:reslib:action:failed"

    # Rollback events
    ROLLBACK_STARTED: str = "res:event:reslib:rollback:started"
    ROLLBACK_SUCCESS: str = "res:event:reslib:rollback:success"
    ROLLBACK_FAILED: str = "res:event:reslib:rollback:failed"


AsyncFunc = Callable[..., Awaitable[None]]
