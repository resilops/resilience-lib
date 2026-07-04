import pytest

from reslib.constants import QuantitySelectionModeEnum
from reslib.exceptions import QuantitySelectionError
from reslib.schemas.validators import QuantitySelection


def test_percentage_selection_enforces_upper_bound():
    with pytest.raises(QuantitySelectionError, match="between 1 and 100"):
        QuantitySelection(mode=QuantitySelectionModeEnum.PERCENTAGE, amount=101)


def test_percentage_selection_resolves_floor_value():
    selection = QuantitySelection(
        mode=QuantitySelectionModeEnum.PERCENTAGE,
        amount=25,
    )

    assert selection.with_total(10) == 2


def test_absolute_selection_rejects_values_above_total():
    selection = QuantitySelection(
        mode=QuantitySelectionModeEnum.ABSOLUTE,
        amount=5,
    )

    with pytest.raises(QuantitySelectionError, match="exceeds the available total"):
        selection.with_total(3)
