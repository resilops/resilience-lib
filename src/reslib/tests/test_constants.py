from reslib.constants import (
    SUPPORTED_HPA_METRIC_SOURCES,
    SUPPORTED_HPA_RESOURCE_TYPES,
    EventEnum,
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
    K8DeploymentKind,
    MetricsEnum,
    QuantitySelectionModeEnum,
    WorkloadStatusEnum,
)


def test_hpa_supported_sets_match_exported_enums():
    assert SUPPORTED_HPA_METRIC_SOURCES == {HpaMetricSourceEnum.RESOURCE}
    assert SUPPORTED_HPA_RESOURCE_TYPES == {HpaResourceTypeEnum.CPU}


def test_enum_values_are_exposed_as_expected():
    assert K8DeploymentKind.DEPLOYMENT.value == "Deployment"
    assert QuantitySelectionModeEnum.PERCENTAGE.value == "percentage"
    assert EventEnum.ACTION_FAILED.value == "res:reslib:event:action:failed"
    assert MetricsEnum.HTTP.value == "res:reslib:metric:http"
    assert WorkloadStatusEnum.healthy.value == "healthy"
