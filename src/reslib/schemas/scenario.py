from typing import Optional, Any, Dict
from pydantic import BaseModel, Field


class BaseSpec(BaseModel):
    """Base specification for a function name and its arguments."""

    name: str = Field(..., description="Name of the function to execute.")
    kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Keyword arguments passed to the function."
    )


class BaseOptionalSpec(BaseModel):
    """Base optional specification for a function name and its arguments."""

    name: Optional[str] = Field(
        default=None,
        description="Name of the function to execute."
    )
    kwargs: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Keyword arguments passed to the function."
    )


class ActionSpec(BaseSpec):
    """Specification of a resilience action (e.g., pod termination, node drain)."""


class ObserverSpec(BaseSpec):
    """Specification of an observer to monitor system behavior."""

    sampling_interval: int = Field(
        default=5, ge=3, description="Interval between observer samples in seconds."
    )

    warmup_period: int = Field(
        default=0, description="Observer warmup period in seconds"
    )
    grace_period: int = Field(
        default=0, description="Grace period in seconds"
    )


class RollbackSpec(BaseOptionalSpec):
    """Specification of the rollback action to execute."""


class GuardRailSpec(BaseOptionalSpec):
    """Specification of the guardrail (precondition) to execute."""
