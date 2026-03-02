from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class BaseError(Exception):
    """Base exception for AI- and API-friendly errors."""

    error_code: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)
    fix_hint: Optional[str] = None
    retryable: bool = False

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def __str__(self) -> str:
        return f"[{self.error_code}] {self.message}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.__class__.__name__,
            "error_code": self.error_code,
            "message": self.message,
            "context": self.context,
            "retryable": self.retryable,
            **({"fix_hint": self.fix_hint} if self.fix_hint else {}),
        }


class FunctionNotFound(Exception):
    pass


class InvalidAsyncHandler(Exception):
    pass


class NotSupportedError(BaseError):
    pass


class ScenarioContextError(Exception):
    pass


class TaskGroupTimeoutError(BaseError):
    pass


class TaskTimeoutError(BaseError):
    pass


class QuantitySelectionError(BaseError):
    pass
