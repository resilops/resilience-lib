from types import SimpleNamespace

import pytest

from reslib.constants import QuantitySelectionModeEnum, WorkloadStatusEnum
from reslib.core.context import get_context, scenario_context
from reslib.guardrails.availability import validate_min_remaining_replicas
from reslib.k8s.exceptions import DisruptionExceedMinAvailabilityError
from reslib.k8s.schema import WorkloadRuntimeState, WorkloadState


def _workload_state(ready_replicas: int) -> WorkloadState:
    return WorkloadState(
        spec={
            "name": "checkout-api",
            "kind": "Deployment",
            "replicas": ready_replicas,
        },
        runtime=WorkloadRuntimeState(
            ready_replicas=ready_replicas,
            status=WorkloadStatusEnum.healthy,
        ),
    )


def _scenario(quantity: int, min_remaining_replicas: int = 1):
    return SimpleNamespace(
        template=SimpleNamespace(
            workload="checkout-api",
            mode=QuantitySelectionModeEnum.ABSOLUTE,
            quantity=quantity,
            min_remaining_replicas=min_remaining_replicas,
        )
    )


@pytest.mark.asyncio
async def test_validate_min_remaining_replicas_allows_safe_disruption():
    async with scenario_context(
        workload=_workload_state(ready_replicas=3),
        scenario=_scenario(quantity=1, min_remaining_replicas=1),
    ):
        await validate_min_remaining_replicas()

        assert get_context("scenario").template.workload == "checkout-api"


@pytest.mark.asyncio
async def test_validate_min_remaining_replicas_blocks_total_outage():
    async with scenario_context(
        workload=_workload_state(ready_replicas=1),
        scenario=_scenario(quantity=1, min_remaining_replicas=1),
    ):
        with pytest.raises(
            DisruptionExceedMinAvailabilityError, match="no ready replicas"
        ):
            await validate_min_remaining_replicas()


@pytest.mark.asyncio
async def test_validate_min_remaining_replicas_blocks_below_minimum():
    async with scenario_context(
        workload=_workload_state(ready_replicas=3),
        scenario=_scenario(quantity=2, min_remaining_replicas=2),
    ):
        with pytest.raises(
            DisruptionExceedMinAvailabilityError, match="below the required minimum"
        ):
            await validate_min_remaining_replicas()
