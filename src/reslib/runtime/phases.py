from enum import Enum


class ExecutionPhase(str, Enum):

    GUARDRAIL = "guardrail"
    OBSERVER = "observer"
    ACTION = "action"
    ROLLBACK = "rollback"
