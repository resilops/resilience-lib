# resilience-lib

`resilience-lib` is the Python runtime library used by resilopshq.com to execute
Kubernetes resilience scenarios. It validates a target workload, runs a
controlled disruption, observes application behavior during the experiment, and
waits for the workload to recover.

The library is intentionally phase-based:

1. **Guardrails** check whether the experiment is safe to run.
2. **Observers** collect measurements while the experiment is active.
3. **Actions** inject the disruption.
4. **Rollbacks** verify that the workload returns to the expected state.
5. **Telemetry** emits structured events and metrics throughout execution.

## Current capabilities

The current implementation focuses on Kubernetes `Deployment` workloads.

Built-in scenario templates:

| Template | Purpose |
| --- | --- |
| `pod_recovery` | Terminate one or more workload pods and verify that replacement pods become ready. |
| `pod_eviction` | Evict one or more workload pods through the Kubernetes eviction API and verify that replacement pods become ready. |
| `rolling_restart` | Restart a workload and verify the rollout completes with current image, config, secrets, and dependencies. |
| `hpa_cpu_stress` | Run CPU stress inside selected pods to trigger HPA scale-up, then verify scale-down/stabilization. |

Built-in guardrails:

| Function | Purpose |
| --- | --- |
| `ensure_workload_steady` | Blocks execution if the workload is reconciling, unavailable, or degraded. |
| `ensure_minimum_replicas` | Ensures the workload has the configured number of desired and ready replicas. Defaults to two. |
| `validate_min_remaining_replicas` | Ensures a pod disruption keeps enough ready replicas alive. |
| `ensure_pdb_not_violated` | Ensures the planned pod disruption does not violate the workload PDB. For endpoint drain, missing PDBs are allowed and an existing PDB is checked against one drained endpoint. |
| `ensure_hpa_exists` | Ensures the target workload has an HPA. |
| `ensure_hpa_not_at_max_replicas` | Blocks HPA testing if the workload is already at max replicas. |
| `validate_metric_and_resource` | Ensures the requested HPA metric source and resource type are supported. |
| `validate_hpa_resource_metric` | Ensures the workload HPA defines the requested metric. |
| `validate_pods_to_stress_cpu` | Ensures CPU stress leaves the configured percentage of pods idle. |
| `ensure_metrics_server_available` | Ensures the Kubernetes Metrics Server can return pod metrics. |

Built-in actions:

| Function | Purpose |
| --- | --- |
| `terminate_pods` | Deletes selected workload pods and waits for deletion. |
| `evict_pods` | Evicts selected workload pods through the Kubernetes eviction subresource and waits for removal. |
| `perform_rolling_restart` | Patches the Deployment pod template to trigger a rolling restart. |
| `stress_cpu_hpa` | Executes `stress-ng` inside selected pods until HPA scale-up is observed. |

Built-in observer:

| Function | Purpose |
| --- | --- |
| `measure_endpoint_latency` | Sends HTTP GET requests and emits aggregate latency/error metrics per observer interval. |

Built-in rollbacks:

| Function | Purpose |
| --- | --- |
| `wait_until_pod_respawn` | Waits for the Deployment to reach its desired ready replica count after pod termination. |
| `wait_until_rolling_restart_complete` | Waits for a rolling restart to finish and fails fast on pod/container errors. |
| `wait_for_hpa_scale_down` | Waits for replicas and CPU utilization to stabilize after HPA scale-up. |

## Requirements

- Python `>=3.12,<4.0`
- Poetry
- Access to a Kubernetes cluster
- Permissions to read Deployments, Services, HPAs, PDBs, Pods, Events, and pod
  metrics in the target namespace
- Permissions to delete pods for `pod_recovery`
- Permissions to create pod evictions for `pod_eviction`
- Permissions to exec into pods for `hpa_cpu_stress`
- Metrics Server installed for HPA CPU stress guardrails
- `stress-ng` available in the target container image for `hpa_cpu_stress`

By default the library loads in-cluster Kubernetes configuration. For local
development, set:

```bash
export RESILOPS_RESILIENCE_LIB_IN_CLUSTER_CONFIG=false
```

## Installation

Install from the repository:

```bash
poetry install
```

Or install the package from a local checkout:

```bash
pip install .
```

## Quick start

```python
import asyncio

from reslib.runtime.scenario import execute_resilience_scenario
from reslib.schemas.scenario import ResiliencyScenario


scenario = ResiliencyScenario.model_validate(
    {
        "name": "pod_recovery",
        "title": "Pod recovery test",
        "description": "Terminate one pod and verify the Deployment recovers.",
        "template": {
            "namespace": "default",
            "workload": "checkout-api",
            "quantity": 1,
            "mode": "absolute",
            "min_remaining_replicas": 1,
        },
        "steps": [
            {"type": "guardrail", "name": "ensure_workload_steady"},
            {"type": "guardrail", "name": "validate_min_remaining_replicas"},
            {
                "type": "guardrail",
                "name": "ensure_pdb_not_violated",
                "params": {},
            },
            {
                "type": "action",
                "name": "terminate_pods",
                "params": {"timeout_seconds": 300},
            },
            {
                "type": "rollback",
                "name": "wait_until_pod_respawn",
                "params": {"timeout_seconds": 300},
            },
        ],
        "observer": {
            "name": "measure_endpoint_latency",
            "config": {
                "sampling_interval_seconds": 5,
                "warmup_period_seconds": 10,
                "grace_period_seconds": 10,
            },
            "params": {
                "endpoint": "http://checkout-api.default.svc.cluster.local/health",
                "request_timeout_seconds": 3,
                "requests_per_interval": 3,
            },
        },
    }
)


asyncio.run(execute_resilience_scenario(scenario=scenario))
```

## Scenario model

Every scenario is represented by `ResiliencyScenario`.

| Field | Description |
| --- | --- |
| `name` | Scenario template name. Must currently be `pod_recovery`, `pod_eviction`, `rolling_restart`, or `hpa_cpu_stress`. |
| `title` | Human-readable title. |
| `description` | Human-readable scenario description. |
| `template` | Scenario-wide Kubernetes and disruption configuration. The shape depends on `name`. |
| `steps` | Ordered guardrail, action, and rollback steps. Each step resolves to an exported async handler. |
| `observer` | Observer function, timing configuration, and observer-specific parameters. |

Step objects use this shape:

```json
{
    "type": "guardrail",
    "name": "exported_async_handler_name",
    "params": {"handler_specific": "values"}
}
```

Observer objects use this shape:

```json
{
    "name": "measure_endpoint_latency",
    "config": {
        "sampling_interval_seconds": 5,
        "warmup_period_seconds": 0,
        "grace_period_seconds": 0
    },
    "params": {
        "endpoint": "http://service.namespace.svc.cluster.local/health",
        "request_timeout_seconds": 3,
        "requests_per_interval": 3
    }
}
```

## `pod_recovery` template

```python
{
    "namespace": "default",
    "workload": "checkout-api",
    "quantity": 1,
    "mode": "absolute",
    "min_remaining_replicas": 1,
}
```

| Field | Description |
| --- | --- |
| `namespace` | Kubernetes namespace containing the target Deployment. |
| `workload` | Target Deployment name. |
| `quantity` | Number of pods to terminate, or percentage when `mode` is `percentage`. |
| `mode` | `absolute` or `percentage`. |
| `min_remaining_replicas` | Minimum ready replicas that must remain after pod termination. |

Recommended `pod_recovery` phase sequence:

1. `ensure_workload_steady`
2. `validate_min_remaining_replicas`
3. `ensure_pdb_not_violated`
4. `terminate_pods`
5. `wait_until_pod_respawn`

## `pod_eviction` template

`pod_eviction` uses the same template fields as `pod_recovery`:

```python
{
    "namespace": "default",
    "workload": "checkout-api",
    "quantity": 1,
    "mode": "absolute",
    "min_remaining_replicas": 1,
}
```

Recommended `pod_eviction` phase sequence:

1. `ensure_workload_steady`
2. `validate_min_remaining_replicas`
3. `ensure_pdb_not_violated`
4. `evict_pods`
5. `wait_until_pod_respawn`

## `rolling_restart` template

```python
{
    "namespace": "default",
    "workload": "checkout-api",
}
```

Recommended `rolling_restart` phase sequence:

1. `ensure_workload_steady`
2. `ensure_minimum_replicas`
3. `ensure_hpa_not_at_max_replicas`
4. `perform_rolling_restart`
5. `wait_until_rolling_restart_complete`

## `hpa_cpu_stress` template

```python
{
    "namespace": "default",
    "workload": "checkout-api",
    "container_name": "app",
    "metric_source": "Resource",
    "resource_type": "cpu",
    "idle_cpu_pct": 10,
    "cpu_stress_threshold_pct": 80,
    "min_idle_pct": 20,
}
```

| Field | Description |
| --- | --- |
| `namespace` | Kubernetes namespace containing the target Deployment. |
| `workload` | Target Deployment name. |
| `container_name` | Optional container name. If omitted, Kubernetes exec uses the default container behavior. |
| `metric_source` | HPA metric source. Currently only `Resource` is supported. |
| `resource_type` | HPA resource type. Currently only `cpu` is supported. |
| `idle_cpu_pct` | Estimated baseline CPU utilization per pod. |
| `cpu_stress_threshold_pct` | CPU load percentage used during stress. Maximum allowed value is `95`. |
| `min_idle_pct` | Percentage of ready pods to leave unstressed. |

Recommended `hpa_cpu_stress` phase sequence:

1. `ensure_workload_steady`
2. `ensure_metrics_server_available`
3. `validate_metric_and_resource`
4. `ensure_hpa_exists`
5. `ensure_hpa_not_at_max_replicas`
6. `validate_hpa_resource_metric`
7. `validate_pods_to_stress_cpu`
8. `stress_cpu_hpa`
9. `wait_for_hpa_scale_down`

Example:

```python
scenario = ResiliencyScenario.model_validate(
    {
        "name": "hpa_cpu_stress",
        "title": "HPA CPU scale test",
        "description": "Apply CPU pressure and verify HPA scale-up and recovery.",
        "template": {
            "namespace": "default",
            "workload": "checkout-api",
            "container_name": "app",
            "metric_source": "Resource",
            "resource_type": "cpu",
            "idle_cpu_pct": 10,
            "cpu_stress_threshold_pct": 80,
            "min_idle_pct": 20,
        },
        "steps": [
            {"type": "guardrail", "name": "ensure_workload_steady"},
            {"type": "guardrail", "name": "ensure_metrics_server_available"},
            {"type": "guardrail", "name": "validate_metric_and_resource"},
            {"type": "guardrail", "name": "ensure_hpa_exists"},
            {"type": "guardrail", "name": "ensure_hpa_not_at_max_replicas"},
            {"type": "guardrail", "name": "validate_hpa_resource_metric"},
            {"type": "guardrail", "name": "validate_pods_to_stress_cpu"},
            {
                "type": "action",
                "name": "stress_cpu_hpa",
                "params": {"max_stress_duration_seconds": 120},
            },
            {
                "type": "rollback",
                "name": "wait_for_hpa_scale_down",
                "params": {"timeout_seconds": 500},
            },
        ],
        "observer": {
            "name": "measure_endpoint_latency",
            "config": {
                "sampling_interval_seconds": 5,
                "warmup_period_seconds": 10,
                "grace_period_seconds": 10,
            },
            "params": {
                "endpoint": "http://checkout-api.default.svc.cluster.local/health",
                "request_timeout_seconds": 3,
                "requests_per_interval": 3,
            },
        },
    }
)
```

## Telemetry

Telemetry is optional. If no telemetry backend is provided,
`NoopTelemetry` silently discards events and metrics.

To collect telemetry, implement `BaseTelemetry`:

```python
from reslib.helpers import BaseTelemetry
from reslib.schemas.telemetry import EventPayload, MetricPayload


class PrintTelemetry(BaseTelemetry):
    def emit_event(self, *, event: EventPayload) -> None:
        print(event.model_dump(mode="json"))

    def emit_metrics(self, *, metrics: MetricPayload) -> None:
        print(metrics.model_dump(mode="json"))
```

Then pass it to the runtime:

```python
await execute_resilience_scenario(
    scenario=scenario,
    telemetry=PrintTelemetry(),
)
```

Event names are defined in `reslib.constants.EventEnum`. HTTP metrics are emitted
as `res:reslib:metric:http` and include aggregate request counts, status-code
counts, latency sums, min/max/avg latency, and cumulative latency buckets.

## Configuration

Configuration is loaded through Pydantic settings using the
`RESILOPS_RESILIENCE_LIB_` environment prefix.

| Environment variable | Default | Description |
| --- | --- | --- |
| `RESILOPS_RESILIENCE_LIB_IN_CLUSTER_CONFIG` | `true` | Load in-cluster Kubernetes configuration. Set to `false` to use local kubeconfig. |
| `RESILOPS_RESILIENCE_LIB_POD_TERMINATION_DEFAULT_GRACE_PERIOD` | `30` | Default pod termination grace period in seconds. |

## Handler resolution

Runtime handlers are resolved by phase and function name through
`reslib.runtime.resolve.resolve`.

Only functions explicitly exported from each phase package are executable. A
handler must also be an async function. This keeps the scenario execution surface
small and prevents accidental execution of internal helpers.

Phase packages:

| Phase | Package |
| --- | --- |
| `guardrail` | `reslib.guardrails` |
| `observer` | `reslib.observers` |
| `action` | `reslib.actions` |
| `rollback` | `reslib.rollbacks` |

## Development

Install dependencies:

```bash
poetry install
```

Run tests:

```bash
poetry run pytest
```

Install pre-commit hooks:

```bash
poetry run pre-commit install
```

Run all pre-commit hooks:

```bash
poetry run pre-commit run --all-files
```

The pre-commit configuration runs YAML checks, end-of-file checks, trailing
whitespace checks, Black, isort, and flake8.

## Project layout

```text
src/reslib/
  actions/       Disruption implementations such as pod deletion and CPU stress.
  core/          Scenario context and async watchdog helpers.
  guardrails/    Preconditions that block unsafe experiments.
  k8s/           Kubernetes client wrappers, workload snapshots, and schemas.
  observers/     Runtime measurement loops.
  rollbacks/     Recovery and stabilization checks.
  runtime/       Scenario execution and handler resolution.
  schemas/       Pydantic scenario, template, telemetry, and validation models.
```

## License

Apache-2.0
