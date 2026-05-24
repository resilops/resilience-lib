from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field, model_validator

from reslib.runtime.phases import ExecutionPhase
from reslib.schemas.templates import (
    SCENARIO_TEMPLATES_MAPPING,
    HpaCpuStressTemplate,
    PodEvictionTemplate,
    PodRecoveryTemplate,
    RollingRestartTemplate,
)


class StepSpec(BaseModel):
    """
    Represents a single step in a scenario.

    Each step can be a:
    - guardrail: precondition check
    - action: fault injection or resilience action
    - rollback: recovery logic
    """

    type: ExecutionPhase = Field(..., description="Type of the step in the scenario.")
    name: str = Field(..., description="Name of the callable or function to execute.")
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Arguments passed to the callable."
    )


class ObserverConfig(BaseModel):
    """
    Configuration for observer timing.

    - `sampling_interval_seconds`: how often the observer polls the system
    - `warmup_period_seconds`: initial period to skip measurements
    - `grace_period_seconds`: period to allow for stabilization after action
    """

    sampling_interval_seconds: int = Field(
        default=5,
        ge=3,
        le=10,
        description="Interval between observer samples in seconds.",
    )
    warmup_period_seconds: int = Field(
        default=0,
        ge=0,
        le=20,
        description="Initial warmup period before measurements in seconds.",
    )
    grace_period_seconds: int = Field(
        default=0,
        ge=0,
        le=20,
        description="Grace period to allow system stabilization in seconds.",
    )


class ObserverSpec(BaseModel):
    """
    Observer definition for monitoring system behavior during scenario execution.

    - `name`: observer callable name
    - `config`: timing configuration
    - `params`: callable-specific arguments
    """

    name: str = Field(..., description="Name of the observer callable.")
    config: ObserverConfig = Field(
        default_factory=ObserverConfig,
        description="Timing and sampling configuration for the observer.",
    )
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Arguments passed to the observer callable."
    )


class ResiliencyScenario(BaseModel):
    """
    Full scenario definition including:

    - `template`: scenario-wide arguments to be merged with all steps
    - `steps`: ordered list of guardrail/action/rollback steps
    - `observer`: monitoring configuration
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    name: str = Field(..., description="Name of the scenario template.")
    title: str = Field(..., description="Title of the scenario template.")
    description: str = Field(..., description="Description of the scenario template.")
    template: (
        PodRecoveryTemplate
        | PodEvictionTemplate
        | RollingRestartTemplate
        | HpaCpuStressTemplate
    ) = Field(..., description="Scenario-specific template fields.")
    steps: List[StepSpec] = Field(
        ..., description="Ordered list of steps (guardrail/action/rollback)."
    )
    observer: ObserverSpec = Field(
        ..., description="Observer configuration to monitor system behavior."
    )

    @model_validator(mode="before")
    @classmethod
    def validate_and_cast_template(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        Dynamically select and validate the scenario template model based on `name`.

        This validator executes before standard model validation. It inspects
        the `name` field to determine which concrete template model should be
        used (e.g. PodRecoveryTemplate, HpaCpuStressTemplate).

        The raw `template` dictionary is then validated against the selected
        template model and replaced with a strongly-typed instance.

        This ensures:
            - Strict schema validation per scenario type
            - No acceptance of arbitrary or mismatched template fields
            - Clear error reporting for unsupported scenario types
            - Consistent typing of `template` across the system

        Raises:
            ValueError:
                - If the scenario type is not registered
        """
        scenario_name: str = values.get("name")
        template_data: Dict[str, Any] = values.get("template")
        template_model = SCENARIO_TEMPLATES_MAPPING.get(scenario_name)

        if not template_model:
            raise ValueError(f"Scenario template {scenario_name} not found.")

        values["template"] = template_model.model_validate(template_data)
        return values
