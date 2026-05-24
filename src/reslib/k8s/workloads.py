import asyncio
from datetime import datetime
from typing import List, Optional

from cachetools import TTLCache
from cachetools.keys import hashkey
from kubernetes.client import V1Deployment, V1Probe, V1Service
from kubernetes.client.exceptions import ApiException

from reslib.constants import (
    DEPLOYMENT_CONDITION_AVAILABLE,
    DEPLOYMENT_CONDITION_PROGRESSING,
    DEPLOYMENT_STATUS_MIN_RS_AVAILABLE,
    DEPLOYMENT_STATUS_PROGRESS_DEADLINE,
    DEPLOYMENT_STATUS_RS_AVAILABLE,
    K8DeploymentKind,
    WorkloadStatusEnum,
)
from reslib.core.context import set_context
from reslib.k8s.client import KubernetesClient
from reslib.k8s.exceptions import (
    RollingRestartCompleteError,
    WorkloadFaultyError,
    WorkloadNotFound,
)
from reslib.k8s.pods import get_latest_pod_ready_time, get_pods_by_labels
from reslib.k8s.schema import (
    ContainerHealthSpec,
    ContainerSpec,
    HPAConfig,
    HPAMetricSpec,
    K8Condition,
    PDBConfig,
    ProbeHttpGet,
    ResourceRequirements,
    WorkloadPolicies,
    WorkloadRuntimeState,
    WorkloadSpec,
    WorkloadState,
)
from reslib.k8s.snapshot import NamespaceSnapshot

_namespace_cache = TTLCache(maxsize=64, ttl=30)
_namespace_cache_lock = asyncio.Lock()


def get_deployment_conditions(deployment: V1Deployment) -> List[Optional[K8Condition]]:
    """Convert Kubernetes Deployment conditions into JSON-friendly models."""
    conditions: List[Optional[K8Condition]] = []
    for condition in deployment.status.conditions or []:
        conditions.append(
            K8Condition(
                type=condition.type,
                status=condition.status,
                reason=getattr(condition, "reason", None),
                message=getattr(condition, "message", None),
                last_transition_time=getattr(condition, "last_transition_time", None),
            )
        )
    return conditions


def is_deployment_in_progress(deployment: V1Deployment) -> bool:
    """Return whether a deployment rollout is still progressing."""
    for condition in deployment.status.conditions or []:
        if (
            condition.type == DEPLOYMENT_CONDITION_PROGRESSING
            and condition.status == "True"
            and condition.reason != DEPLOYMENT_STATUS_RS_AVAILABLE
        ):
            return True
    return False


def is_deployment_available(deployment: V1Deployment) -> bool:
    """Return whether a deployment is currently available."""
    for condition in deployment.status.conditions or []:
        if (
            condition.type == DEPLOYMENT_CONDITION_AVAILABLE
            and condition.status == "True"
            and condition.reason == DEPLOYMENT_STATUS_MIN_RS_AVAILABLE
        ):
            return True
    return False


def is_deployment_faulty(deployment: V1Deployment) -> bool:
    """Return whether a deployment rollout failed its progress deadline."""
    for condition in deployment.status.conditions or []:
        if (
            condition.type == DEPLOYMENT_CONDITION_PROGRESSING
            and condition.status == "False"
            and condition.reason == DEPLOYMENT_STATUS_PROGRESS_DEADLINE
        ):
            return True
    return False


async def raise_on_rolling_restart_complete(
    *,
    k8s: KubernetesClient,
    workload_name: str,
    namespace: str,
    started_at: str,
    target_generation: Optional[int],
) -> None:
    """Raise when a Deployment has completed the requested rolling restart."""
    deployment = await k8s.read_namespaced_deployment(
        name=workload_name,
        namespace=namespace,
    )

    if is_deployment_faulty(deployment):
        raise WorkloadFaultyError(
            error_code="ROLLING_RESTART_FAILED",
            message=(
                f"Deployment '{workload_name}' failed while rolling restart was "
                "in progress."
            ),
            fix_hint=(
                "Inspect rollout status, deployment events, pod events, and "
                "container logs for image, config, secret, or dependency failures."
            ),
        )

    desired_replicas = deployment.spec.replicas or 0
    observed_generation = deployment.status.observed_generation or 0
    updated_replicas = deployment.status.updated_replicas or 0
    ready_replicas = deployment.status.ready_replicas or 0
    available_replicas = deployment.status.available_replicas or 0
    unavailable_replicas = deployment.status.unavailable_replicas or 0

    if target_generation is not None and observed_generation < target_generation:
        return None

    if not (
        updated_replicas >= desired_replicas
        and ready_replicas >= desired_replicas
        and available_replicas >= desired_replicas
        and unavailable_replicas == 0
    ):
        return None

    pods = await get_pods_by_labels(
        k8s=k8s,
        namespace=namespace,
        labels=deployment.spec.selector.match_labels or {},
        pod_phase=None,
    )
    latest_pod_ready_time = get_latest_pod_ready_time(pods)
    if (
        latest_pod_ready_time is None
        or latest_pod_ready_time <= datetime.fromisoformat(started_at)
    ):
        return None

    observed = {
        "desired_replicas": desired_replicas,
        "updated_replicas": updated_replicas,
        "ready_replicas": ready_replicas,
        "available_replicas": available_replicas,
        "observed_generation": observed_generation,
        "rolling_restart_generation": target_generation,
        "latest_pod_ready_time": latest_pod_ready_time.isoformat(),
    }
    set_context("rolling_restart_complete", observed)
    raise RollingRestartCompleteError(
        error_code="ROLLING_RESTART_COMPLETE",
        message=(
            f"Deployment '{workload_name}' completed rolling restart with "
            f"{ready_replicas} ready replica(s)."
        ),
    )


async def get_namespace_policies_snapshot(
    k8s: KubernetesClient, namespace: str
) -> NamespaceSnapshot:
    """Fetch and cache namespace-scoped HPA and PDB information."""
    cache_key = hashkey(k8s, namespace)
    cached_snapshot = _namespace_cache.get(cache_key)
    if cached_snapshot is not None:
        return cached_snapshot

    hpa_list, pdb_list = await asyncio.gather(
        k8s.list_namespaced_horizontal_pod_autoscaler(namespace=namespace),
        k8s.list_namespaced_pod_disruption_budget(namespace=namespace),
    )

    snapshot = NamespaceSnapshot(
        hpas={hpa.spec.scale_target_ref.name: hpa for hpa in hpa_list.items},
        pdbs=pdb_list.items,
    )

    async with _namespace_cache_lock:
        existing_snapshot = _namespace_cache.get(cache_key)
        if existing_snapshot is not None:
            return existing_snapshot
        _namespace_cache[cache_key] = snapshot

    return snapshot


def _resolve_service_port_for_probe(probe_port, service) -> Optional[int]:
    """Resolve service port"""
    if not service or not service.spec or not service.spec.ports:
        return probe_port

    for svc_port in service.spec.ports:
        # Service targetPort points to the container/probe port
        target_port = getattr(svc_port, "target_port", None)

        if target_port == probe_port:
            return svc_port.port

    return probe_port


def _build_probe_http_get(
    probe: Optional[V1Probe],
    service: Optional[V1Service],
    namespace: Optional[str],
) -> Optional[ProbeHttpGet]:
    """Build a normalized HTTP probe endpoint definition."""
    if not probe or not probe.http_get:
        return None

    http_get = probe.http_get
    host = getattr(http_get, "host", None)
    service_name = service.metadata.name if service is not None else None

    if not host and service_name and namespace:
        host = f"{service_name}.{namespace}.svc.cluster.local"

    probe_port = getattr(http_get, "port", None)
    service_port = _resolve_service_port_for_probe(probe_port, service)

    return ProbeHttpGet(
        path=getattr(http_get, "path", None) or "/",
        port=service_port,
        host=host,
        scheme=(getattr(http_get, "scheme", None) or "HTTP").lower(),
    )


def _build_container_specs(
    deployment: V1Deployment,
    service: Optional[V1Service],
    *,
    include_resources: bool,
) -> List[ContainerSpec]:
    """Build container models from a deployment pod template."""
    containers: List[ContainerSpec] = []
    pod_spec = deployment.spec.template.spec
    namespace = deployment.metadata.namespace

    for container in pod_spec.containers or []:
        resources = getattr(container, "resources", None)
        containers.append(
            ContainerSpec(
                name=container.name,
                resources=(
                    ResourceRequirements(
                        requests=getattr(resources, "requests", None),
                        limits=getattr(resources, "limits", None),
                    )
                    if resources and include_resources
                    else None
                ),
                health=ContainerHealthSpec(
                    readiness=_build_probe_http_get(
                        getattr(container, "readiness_probe", None),
                        service,
                        namespace,
                    ),
                    liveness=_build_probe_http_get(
                        getattr(container, "liveness_probe", None),
                        service,
                        namespace,
                    ),
                    startup=(
                        _build_probe_http_get(
                            getattr(container, "startup_probe", None),
                            service,
                            namespace,
                        )
                        if include_resources
                        else None
                    ),
                ),
            )
        )

    return containers


def _build_hpa_config(
    deployment: V1Deployment,
    snapshot: Optional[NamespaceSnapshot],
) -> Optional[HPAConfig]:
    """Build HPA config for a deployment using the namespace snapshot."""
    hpa = snapshot.get_hpa(deployment) if snapshot else None
    if hpa is None:
        return None

    return HPAConfig(
        name=hpa.metadata.name,
        min_replicas=hpa.spec.min_replicas,
        max_replicas=hpa.spec.max_replicas,
        metrics=[
            HPAMetricSpec(type=metric.type, resource=metric.resource.to_dict())
            for metric in (hpa.spec.metrics or [])
        ],
    )


def get_service(
    deployment: V1Deployment,
    services: list[V1Service] | None = None,
) -> V1Service | None:
    """Return the deterministic service targeting deployment pods."""
    if not services:
        return None

    pod_labels = deployment.spec.template.metadata.labels or {}

    matching_services = [
        service
        for service in services
        if service.spec.selector
        and all(
            pod_labels.get(key) == value for key, value in service.spec.selector.items()
        )
    ]

    if not matching_services:
        return None

    return min(matching_services, key=lambda service: service.metadata.name)


def get_workload_spec(
    *,
    deployment: V1Deployment,
    snapshot: Optional[NamespaceSnapshot] = None,
    services: Optional[List[V1Service]] = None,
    is_full: bool = True,
) -> WorkloadSpec:
    """Build a normalized workload spec from a deployment."""
    service = get_service(deployment, services)
    service_name = service.metadata.name if service is not None else None
    return WorkloadSpec(
        name=deployment.metadata.name,
        service_name=service_name,
        kind=K8DeploymentKind.DEPLOYMENT,
        replicas=deployment.spec.replicas or 0,
        hpa=_build_hpa_config(deployment, snapshot) if is_full else None,
        labels=deployment.spec.selector.match_labels if is_full else None,
        containers=_build_container_specs(
            deployment,
            service,
            include_resources=is_full,
        ),
    )


def get_workload_policies(
    snapshot: NamespaceSnapshot,
    deployment: V1Deployment,
) -> WorkloadPolicies:
    """Build workload policies from snapshot data."""
    pod_labels = deployment.spec.template.metadata.labels or {}
    pdb = snapshot.get_pdb(pod_labels)
    return WorkloadPolicies(
        pdb=(
            PDBConfig(
                min_available=pdb.spec.min_available,
                max_unavailable=pdb.spec.max_unavailable,
            )
            if pdb
            else None
        )
    )


def get_workload_runtime(
    deployment: V1Deployment,
    is_full: bool = True,
) -> WorkloadRuntimeState:
    """Build the current runtime state for a deployment."""
    if is_deployment_faulty(deployment):
        status = WorkloadStatusEnum.degraded
    elif is_deployment_in_progress(deployment):
        status = WorkloadStatusEnum.reconciling
    elif is_deployment_available(deployment):
        status = WorkloadStatusEnum.healthy
    else:
        status = WorkloadStatusEnum.unavailable

    return WorkloadRuntimeState(
        ready_replicas=deployment.status.ready_replicas or 0,
        status=status,
        generation=deployment.metadata.generation,
        observed_generation=deployment.status.observed_generation,
        conditions=get_deployment_conditions(deployment) if is_full else None,
    )


async def get_workload(
    *,
    namespace: str,
    name: str,
    k8s: Optional[KubernetesClient] = None,
) -> WorkloadState:
    """Fetch a single workload by name and build its normalized state."""
    k8s = k8s or KubernetesClient()
    snapshot, service_list = await asyncio.gather(
        get_namespace_policies_snapshot(k8s=k8s, namespace=namespace),
        k8s.list_namespaced_service(namespace=namespace),
    )

    try:
        deployment = await k8s.read_namespaced_deployment(
            name=name,
            namespace=namespace,
        )
    except ApiException as exc:
        if exc.status == 404:
            raise WorkloadNotFound(
                error_code="WORKLOAD_NOT_FOUND",
                message=(
                    f"Deployment '{name}' was not found in namespace " f"'{namespace}'."
                ),
                fix_hint=(
                    "Verify the deployment name and namespace, or create the "
                    "workload before retrying."
                ),
            )
        raise

    return WorkloadState(
        spec=get_workload_spec(
            deployment=deployment,
            snapshot=snapshot,
            services=service_list.items,
        ),
        policies=get_workload_policies(snapshot=snapshot, deployment=deployment),
        runtime=get_workload_runtime(deployment),
    )
