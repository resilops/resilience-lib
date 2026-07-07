from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kubernetes.client.rest import ApiException

from reslib.constants import (
    ENDPOINT_DRAIN_SCENARIO_TEMPLATE,
    HpaMetricSourceEnum,
    HpaResourceTypeEnum,
    QuantitySelectionModeEnum,
    WorkloadStatusEnum,
)
from reslib.core.context import scenario_context
from reslib.exceptions import NotSupportedError
from reslib.guardrails import (
    hpa as hpa_guardrails,
    metrics as metrics_guardrails,
    pdb as pdb_guardrails,
    workload as workload_guardrails,
)
from reslib.k8s.exceptions import (
    DisruptionExceedMinAvailabilityError,
    HpaMetricsNotFoundError,
    HpaNotConfiguredError,
    InsufficientReplicasError,
    MetricsServerUnavailableError,
    PdbNotConfiguredError,
    PodsToStressExceededError,
    WorkloadAtMaxError,
    WorkloadFaultyError,
    WorkloadNotAvailableError,
    WorkloadReconcilingError,
    WorkloadStatusUnavailableError,
)


def _scenario(
    *,
    name="hpa_cpu_stress",
    namespace="payments",
    workload="checkout",
    metric_source=HpaMetricSourceEnum.RESOURCE,
    resource_type=HpaResourceTypeEnum.CPU,
    mode=QuantitySelectionModeEnum.ABSOLUTE,
    quantity=1,
    idle_cpu_pct=20,
    cpu_stress_threshold_pct=70,
    min_idle_pct=50,
):
    return SimpleNamespace(
        name=name,
        template=SimpleNamespace(
            namespace=namespace,
            workload=workload,
            metric_source=metric_source,
            resource_type=resource_type,
            mode=mode,
            quantity=quantity,
            idle_cpu_pct=idle_cpu_pct,
            cpu_stress_threshold_pct=cpu_stress_threshold_pct,
            min_idle_pct=min_idle_pct,
        ),
    )


def _workload(
    *,
    name="checkout",
    replicas=3,
    ready_replicas=3,
    status=WorkloadStatusEnum.healthy,
    hpa=None,
    pdb=None,
):
    return SimpleNamespace(
        spec=SimpleNamespace(name=name, replicas=replicas, hpa=hpa),
        runtime=SimpleNamespace(ready_replicas=ready_replicas, status=status),
        policies=SimpleNamespace(pdb=pdb) if pdb is not None else None,
    )


@pytest.mark.asyncio
async def test_validate_metric_and_resource_accepts_supported_values():
    async with scenario_context(scenario=_scenario()):
        assert await hpa_guardrails.validate_metric_and_resource() is None


@pytest.mark.asyncio
async def test_validate_metric_and_resource_rejects_unsupported_metric_source():
    async with scenario_context(
        scenario=_scenario(metric_source=HpaMetricSourceEnum.PODS)
    ):
        with pytest.raises(NotSupportedError, match="not supported"):
            await hpa_guardrails.validate_metric_and_resource()


@pytest.mark.asyncio
async def test_validate_metric_and_resource_rejects_unsupported_resource_type():
    async with scenario_context(
        scenario=_scenario(resource_type=HpaResourceTypeEnum.MEMORY)
    ):
        with pytest.raises(NotSupportedError, match="not supported"):
            await hpa_guardrails.validate_metric_and_resource()


@pytest.mark.asyncio
async def test_validate_hpa_resource_metric_raises_when_metric_missing(monkeypatch):
    monkeypatch.setattr(hpa_guardrails, "get_hpa_resource_metric", lambda **_: None)

    async with scenario_context(
        workload=_workload(hpa=SimpleNamespace()),
        scenario=_scenario(),
    ):
        with pytest.raises(HpaMetricsNotFoundError, match="does not define"):
            await hpa_guardrails.validate_hpa_resource_metric()


@pytest.mark.asyncio
async def test_validate_hpa_resource_metric_returns_none_when_metric_exists(
    monkeypatch,
):
    monkeypatch.setattr(
        hpa_guardrails, "get_hpa_resource_metric", lambda **_: SimpleNamespace()
    )

    async with scenario_context(
        workload=_workload(hpa=SimpleNamespace()),
        scenario=_scenario(),
    ):
        assert await hpa_guardrails.validate_hpa_resource_metric() is None


@pytest.mark.asyncio
async def test_ensure_hpa_exists_raises_without_hpa():
    async with scenario_context(workload=_workload(hpa=None), scenario=_scenario()):
        with pytest.raises(HpaNotConfiguredError, match="does not have an HPA"):
            await hpa_guardrails.ensure_hpa_exists()


@pytest.mark.asyncio
async def test_ensure_hpa_not_at_max_replicas_handles_all_paths():
    async with scenario_context(workload=_workload(hpa=None), scenario=_scenario()):
        assert await hpa_guardrails.ensure_hpa_not_at_max_replicas() is None

    async with scenario_context(
        workload=_workload(ready_replicas=3, hpa=SimpleNamespace(max_replicas=3)),
        scenario=_scenario(),
    ):
        with pytest.raises(WorkloadAtMaxError, match="already at 3 ready replica"):
            await hpa_guardrails.ensure_hpa_not_at_max_replicas()

    async with scenario_context(
        workload=_workload(ready_replicas=2, hpa=SimpleNamespace(max_replicas=3)),
        scenario=_scenario(),
    ):
        assert await hpa_guardrails.ensure_hpa_not_at_max_replicas() is None


@pytest.mark.asyncio
async def test_validate_pods_to_stress_cpu_enforces_min_idle_budget(monkeypatch):
    monkeypatch.setattr(
        hpa_guardrails, "get_hpa_resource_metric", lambda **_: SimpleNamespace()
    )
    monkeypatch.setattr(hpa_guardrails, "calculate_hpa_trigger", lambda **_: (3, 80))

    async with scenario_context(
        workload=_workload(ready_replicas=4, hpa=SimpleNamespace()),
        scenario=_scenario(min_idle_pct=50),
    ):
        with pytest.raises(PodsToStressExceededError, match="needs to stress 3 pod"):
            await hpa_guardrails.validate_pods_to_stress_cpu()


@pytest.mark.asyncio
async def test_validate_pods_to_stress_cpu_allows_safe_selection(monkeypatch):
    monkeypatch.setattr(
        hpa_guardrails, "get_hpa_resource_metric", lambda **_: SimpleNamespace()
    )
    monkeypatch.setattr(hpa_guardrails, "calculate_hpa_trigger", lambda **_: (1, 80))

    async with scenario_context(
        workload=_workload(ready_replicas=4, hpa=SimpleNamespace()),
        scenario=_scenario(min_idle_pct=50),
    ):
        assert await hpa_guardrails.validate_pods_to_stress_cpu() is None


@pytest.mark.asyncio
async def test_ensure_metrics_server_available_handles_success_and_errors(monkeypatch):
    pod = SimpleNamespace(metadata=SimpleNamespace(name="pod-a"))
    fake_k8s = SimpleNamespace(
        get_namespaced_custom_object=AsyncMock(
            return_value={"containers": [{"cpu": "1m"}]}
        )
    )
    monkeypatch.setattr(metrics_guardrails, "KubernetesClient", lambda: fake_k8s)
    monkeypatch.setattr(
        metrics_guardrails, "get_workload_pods", AsyncMock(return_value=[pod])
    )

    async with scenario_context(
        workload=_workload(),
        scenario=_scenario(namespace="payments"),
    ):
        assert await metrics_guardrails.ensure_metrics_server_available() is None

    fake_k8s.get_namespaced_custom_object = AsyncMock(
        side_effect=ApiException(status=503, reason="unavailable")
    )
    async with scenario_context(
        workload=_workload(),
        scenario=_scenario(namespace="payments"),
    ):
        with pytest.raises(MetricsServerUnavailableError, match="Metrics API"):
            await metrics_guardrails.ensure_metrics_server_available()

    fake_k8s.get_namespaced_custom_object = AsyncMock(side_effect=RuntimeError("boom"))
    async with scenario_context(
        workload=_workload(),
        scenario=_scenario(namespace="payments"),
    ):
        with pytest.raises(
            MetricsServerUnavailableError,
            match="METRICS_SERVER_QUERY_UNEXPECTED_ERROR",
        ):
            await metrics_guardrails.ensure_metrics_server_available()

    fake_k8s.get_namespaced_custom_object = AsyncMock(return_value={"containers": []})
    async with scenario_context(
        workload=_workload(),
        scenario=_scenario(namespace="payments"),
    ):
        with pytest.raises(MetricsServerUnavailableError, match="no container metrics"):
            await metrics_guardrails.ensure_metrics_server_available()


@pytest.mark.asyncio
async def test_ensure_pdb_not_violated_covers_missing_and_violation_paths():
    async with scenario_context(
        workload=_workload(pdb=None),
        scenario=_scenario(name=ENDPOINT_DRAIN_SCENARIO_TEMPLATE),
    ):
        assert await pdb_guardrails.ensure_pdb_not_violated() is None

    async with scenario_context(workload=_workload(pdb=None), scenario=_scenario()):
        with pytest.raises(
            PdbNotConfiguredError, match="does not have a PodDisruptionBudget"
        ):
            await pdb_guardrails.ensure_pdb_not_violated()

    async with scenario_context(
        workload=_workload(
            ready_replicas=4,
            pdb=SimpleNamespace(max_unavailable=1, min_available=None),
        ),
        scenario=_scenario(quantity=2),
    ):
        with pytest.raises(
            DisruptionExceedMinAvailabilityError, match="maximum unavailable count"
        ):
            await pdb_guardrails.ensure_pdb_not_violated()

    async with scenario_context(
        workload=_workload(
            ready_replicas=3,
            pdb=SimpleNamespace(max_unavailable=None, min_available=3),
        ),
        scenario=_scenario(quantity=1),
    ):
        with pytest.raises(
            DisruptionExceedMinAvailabilityError,
            match="below the PodDisruptionBudget minimum",
        ):
            await pdb_guardrails.ensure_pdb_not_violated()

    async with scenario_context(
        workload=_workload(
            ready_replicas=3,
            pdb=SimpleNamespace(max_unavailable=None, min_available=2),
        ),
        scenario=_scenario(quantity=1),
    ):
        assert await pdb_guardrails.ensure_pdb_not_violated() is None


@pytest.mark.asyncio
async def test_ensure_workload_steady_covers_runtime_states():
    async with scenario_context(
        workload=SimpleNamespace(spec=SimpleNamespace(name="checkout"), runtime=None),
        scenario=_scenario(),
    ):
        with pytest.raises(
            WorkloadStatusUnavailableError, match="does not have runtime status"
        ):
            await workload_guardrails.ensure_workload_steady()

    async with scenario_context(
        workload=_workload(status=WorkloadStatusEnum.reconciling),
        scenario=_scenario(),
    ):
        with pytest.raises(WorkloadReconcilingError, match="still reconciling"):
            await workload_guardrails.ensure_workload_steady()

    async with scenario_context(
        workload=_workload(status=WorkloadStatusEnum.unavailable),
        scenario=_scenario(),
    ):
        with pytest.raises(WorkloadNotAvailableError, match="not currently available"):
            await workload_guardrails.ensure_workload_steady()

    async with scenario_context(
        workload=_workload(status=WorkloadStatusEnum.degraded),
        scenario=_scenario(),
    ):
        with pytest.raises(WorkloadFaultyError, match="degraded state"):
            await workload_guardrails.ensure_workload_steady()

    async with scenario_context(
        workload=_workload(status=WorkloadStatusEnum.healthy),
        scenario=_scenario(),
    ):
        assert await workload_guardrails.ensure_workload_steady() is None


@pytest.mark.asyncio
async def test_ensure_minimum_replicas_raises_and_allows_override():
    async with scenario_context(
        workload=_workload(replicas=1, ready_replicas=1),
        scenario=_scenario(name="pod_recovery"),
    ):
        with pytest.raises(
            InsufficientReplicasError,
            match="needs at least 2 desired and ready replicas",
        ):
            await workload_guardrails.ensure_minimum_replicas()

    async with scenario_context(
        workload=_workload(replicas=3, ready_replicas=3),
        scenario=_scenario(name="pod_recovery"),
    ):
        assert await workload_guardrails.ensure_minimum_replicas(min_replicas=3) is None
