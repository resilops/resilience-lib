from typing import Any, Dict, List

from pydantic import BaseModel, Field

from reslib.runtime.phases import ExecutionPhase


class BaseSpec(BaseModel):
    """
    Base specification for a scenario step with a function name
    and optional step-specific overrides.
    """

    name: str = Field(..., description="Name of the callable or function to execute.")
    overrides: Dict[str, Any] = Field(
        default_factory=dict,
        description="Step-specific overrides merged with the scenario template.",
    )


class StepSpec(BaseSpec):
    """
    Represents a single step in a scenario.

    Each step can be a:
    - guardrail: precondition check
    - action: fault injection or resilience action
    - rollback: recovery logic
    """

    type: ExecutionPhase = Field(..., description="Type of the step in the scenario.")


class ObserverConfig(BaseModel):
    """
    Configuration for observer timing.

    - `sampling_interval`: how often the observer polls the system (in seconds)
    - `warmup_period`: initial period to skip measurements (in seconds)
    - `grace_period`: period to allow for stabilization after action (in seconds)
    """

    sampling_interval: int = Field(
        default=5, ge=1, description="Interval between observer samples in seconds."
    )
    warmup_period: int = Field(
        default=0, description="Initial warmup period before measurements in seconds."
    )
    grace_period: int = Field(
        default=0, description="Grace period to allow system stabilization in seconds."
    )


class ObserverSpec(BaseModel):
    """
    Observer definition for monitoring system behavior during scenario execution.

    - `name`: observer callable name
    - `config`: timing configuration
    - `kwargs`: callable-specific arguments
    """

    name: str = Field(..., description="Name of the observer callable.")
    config: ObserverConfig = Field(
        default_factory=ObserverConfig,
        description="Timing and sampling configuration for the observer.",
    )
    kwargs: Dict[str, Any] = Field(
        default_factory=dict, description="Arguments passed to the observer callable."
    )


class ResiliencyScenario(BaseModel):
    """
    Full scenario definition including:

    - `template`: scenario-wide arguments to be merged with all steps
    - `steps`: ordered list of guardrail/action/rollback steps
    - `observer`: monitoring configuration
    """

    template: Dict[str, Any] = Field(
        default_factory=dict,
        description="Scenario-specific template fields merged into all steps.",
    )
    steps: List[StepSpec] = Field(
        ..., description="Ordered list of steps (guardrail/action/rollback)."
    )
    observer: ObserverSpec = Field(
        ..., description="Observer configuration to monitor system behavior."
    )
