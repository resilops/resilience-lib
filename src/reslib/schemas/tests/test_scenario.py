import pytest

from reslib.constants import QuantitySelectionModeEnum
from reslib.runtime.phases import ExecutionPhase
from reslib.schemas.scenario import ResiliencyScenario
from reslib.schemas.templates import PodRecoveryTemplate


def test_resiliency_scenario_casts_template_by_name():
    scenario = ResiliencyScenario.model_validate(
        {
            "name": "pod_recovery",
            "title": "Pod recovery",
            "description": "Terminate one pod",
            "template": {
                "namespace": "default",
                "workload": "checkout-api",
                "quantity": 1,
                "mode": "absolute",
                "min_remaining_replicas": 1,
            },
            "steps": [
                {
                    "type": "guardrail",
                    "name": "ensure_workload_steady",
                    "params": {},
                }
            ],
            "observer": {
                "name": "measure_endpoint_latency",
                "config": {"sampling_interval_seconds": 5},
                "params": {"endpoint": "https://example.com/health"},
            },
        }
    )

    assert isinstance(scenario.template, PodRecoveryTemplate)
    assert scenario.steps[0].type is ExecutionPhase.GUARDRAIL
    assert scenario.template.mode is QuantitySelectionModeEnum.ABSOLUTE


def test_resiliency_scenario_rejects_unknown_template_name():
    with pytest.raises(ValueError, match="not found"):
        ResiliencyScenario.model_validate(
            {
                "name": "unknown",
                "title": "Bad scenario",
                "description": "No template",
                "template": {},
                "steps": [],
                "observer": {
                    "name": "measure_endpoint_latency",
                    "config": {"sampling_interval_seconds": 5},
                    "params": {"endpoint": "https://example.com/health"},
                },
            }
        )
