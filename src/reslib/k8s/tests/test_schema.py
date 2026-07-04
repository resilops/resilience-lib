from reslib.constants import HpaMetricSourceEnum, K8DeploymentKind, WorkloadStatusEnum
from reslib.k8s.schema import (
    HPAMetricSpec,
    ProbeHttpGet,
    ResourceRequirements,
    WorkloadRuntimeState,
    WorkloadSpec,
)


def test_workload_spec_supports_nested_container_data():
    spec = WorkloadSpec(
        name="checkout-api",
        kind=K8DeploymentKind.DEPLOYMENT,
        replicas=3,
        containers=[
            {
                "name": "api",
                "resources": {"requests": {"cpu": "250m"}, "limits": {"cpu": "500m"}},
                "health": {"readiness": ProbeHttpGet(path="/health", port=8080)},
            }
        ],
    )

    assert spec.containers[0].resources == ResourceRequirements(
        requests={"cpu": "250m"},
        limits={"cpu": "500m"},
    )
    assert spec.containers[0].health.readiness.path == "/health"


def test_hpa_metric_spec_and_runtime_state_validate_enums():
    metric = HPAMetricSpec(
        type=HpaMetricSourceEnum.RESOURCE,
        resource={"name": "cpu"},
    )
    runtime = WorkloadRuntimeState(
        ready_replicas=2,
        status=WorkloadStatusEnum.healthy,
    )

    assert metric.type is HpaMetricSourceEnum.RESOURCE
    assert runtime.status is WorkloadStatusEnum.healthy
