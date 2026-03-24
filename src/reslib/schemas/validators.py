import math

from pydantic import BaseModel, Field, model_validator

from reslib.constants import QuantitySelectionModeEnum
from reslib.core.context import get_context
from reslib.exceptions import QuantitySelectionError
from reslib.schemas.scenario import ResiliencyScenario


class QuantitySelection(BaseModel):
    """
    Describes *how* a quantity should be selected.

    This model represents **pure selection intent** and performs only
    basic structural validation. It does **not** enforce safety or
    disruption policies (e.g., PDBs, minimum availability).

    Examples:
        - Select 3 pods absolutely
        - Select 25% of available pods
    """

    mode: QuantitySelectionModeEnum = Field(
        ...,
        description="Selection mode: absolute count or percentage",
    )

    amount: int = Field(..., gt=0, description="Selection value (count or percentage)")

    @model_validator(mode="after")
    def validate_bounds(self):
        """
        Validate that the selection intent is structurally valid.

        Rules:
            - Percentage-based selection must not exceed 100%
        """
        if self.mode == QuantitySelectionModeEnum.PERCENTAGE and self.amount > 100:
            scenario: ResiliencyScenario = get_context("scenario")
            raise QuantitySelectionError(
                error_code="INVALID_PERCENTAGE_SELECTION",
                message="Percentage-based quantity selection exceeds allowed limit.",
                namespace=scenario.template.namespace,
                workload=scenario.template.workload,
                context={
                    "rule": "percentage <= 100",
                    "inputs": {
                        "mode": self.mode.value,
                        "amount": self.amount,
                    },
                    "observed": {
                        "max_allowed_percentage": 100,
                    },
                },
                fix_hint="Provide a percentage value between 1 and 100.",
                retryable=False,
            )
        return self

    def with_total(self, total: int) -> int:
        """
        Resolve the effective quantity given a total available count.

        Args:
            total: Total number of available units (e.g., ready pods)

        Returns:
            The resolved integer quantity to operate on.
        """
        if self.mode == QuantitySelectionModeEnum.PERCENTAGE:
            return math.floor(total * self.amount / 100)

        if self.amount > total:
            scenario: ResiliencyScenario = get_context("scenario")
            raise QuantitySelectionError(
                error_code="ABSOLUTE_SELECTION_EXCEEDS_TOTAL",
                message="Requested absolute quantity exceeds available total.",
                namespace=scenario.template.namespace,
                workload=scenario.template.workload,
                context={
                    "rule": "amount <= total",
                    "inputs": {
                        "amount": self.amount,
                        "total_available": total,
                    },
                    "observed": {
                        "exceeds_total": True,
                    },
                },
                fix_hint=(
                    "Reduce the absolute quantity or increase the available "
                    "replica count before performing the operation."
                ),
                retryable=False,
            )

        return self.amount
