import math

from pydantic import BaseModel, Field, model_validator

from reslib.constants import QuantitySelectionModeEnum
from reslib.exceptions import QuantitySelectionError


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
            raise QuantitySelectionError(
                error_code="INVALID_PERCENTAGE_SELECTION",
                message=(
                    f"Percentage quantity {self.amount}% is invalid. The value must "
                    "be between 1 and 100."
                ),
                fix_hint="Provide a percentage value between 1 and 100.",
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
            raise QuantitySelectionError(
                error_code="ABSOLUTE_SELECTION_EXCEEDS_TOTAL",
                message=(
                    f"Requested quantity {self.amount} exceeds the available total "
                    f"of {total}."
                ),
                fix_hint=(
                    "Reduce the absolute quantity or increase the available "
                    "replica count before performing the operation."
                ),
            )

        return self.amount
