import logging

from reslib.k8s.client import KubernetesClient
from reslib.k8s.schema import WorkloadState
from reslib.k8s.utils import get_workload
from reslib.schemas.hpa import HpaCPUStressArgsTemplate
from reslib.validators.hpa import (
    ensure_hpa_exists,
    ensure_not_at_max_replicas,
    validate_hpa_resource_metric,
    validate_pods_to_stress_cpu,
)
from reslib.validators.metrics import ensure_metrics_server_available
from reslib.validators.workload import ensure_workload_steady

logger = logging.getLogger(__name__)


async def validate_hpa_cpu_scaling_guardrail(**kwargs) -> None:
    """
    Guardrail to validate that a Kubernetes workload is ready for HPA scaling
    experiments.

    This guardrail performs the following checks in sequence:
    1. Resolves the workload object from the cluster.
    2. Ensures the workload is in a steady state (ready, not reconciling, not faulty).
    3. Confirms that HPA is configured for the workload.
    4. Checks that the workload is not already at its HPA maximum replica count.

    Raises:
        WorkloadStatusUnavailableError: If the workload status is unavailable.
        WorkloadReconcilingError: If the workload is currently reconciling.
        WorkloadNotAvailableError: If the workload is not available/stable.
        WorkloadFaultyError: If the workload is faulty.
        HpaNotConfiguredError: If HPA is not configured for the workload.
        WorkloadAtMaxError: If the workload has already reached its HPA max replicas.

    Args:
        **kwargs: Arguments matching HpaScalingGuardrailArgs
            - namespace (str): Kubernetes namespace of the workload.
            - workload (str): Name of the workload.
            - metric_source (HpaMetricSourceEnum): Type of HPA metric.
            - resource (HpaResourceTypeEnum): Resource name (CPU, memory).

    Returns:
        None
    """
    logger.info("Validating hpa cpu scaling guardrail")
    # Parse arguments
    args = HpaCPUStressArgsTemplate(**kwargs)
    k8s = KubernetesClient()

    # Discover the workload in the cluster
    workload: WorkloadState = get_workload(namespace=args.namespace, name=args.workload)

    # Ensure the workload is steady (ready, not reconciling, not faulty)
    ensure_workload_steady(workload=workload)

    # Ensure HPA is configured
    ensure_hpa_exists(workload=workload)

    # Check if hpa scaling behaviour / resource is supported
    validate_hpa_resource_metric(
        hpa=workload.spec.hpa,
        metric_source=args.metric_source,
        resource_type=args.resource_type,
    )

    # Make sure we don't stress pod that can cause downtime
    validate_pods_to_stress_cpu(
        workload=workload,
        metric_source=args.metric_source,
        resource_type=args.resource_type,
        idle_cpu_pct=args.idle_cpu_pct,
        pod_cpu_stress_threshold_pct=args.pod_cpu_stress_threshold_pct,
        min_pods_idle_pct=args.min_pods_idle_pct,
    )

    # Ensure workload is not already at HPA max replicas
    ensure_not_at_max_replicas(workload=workload)

    # Make sure metrics server is available
    ensure_metrics_server_available(
        workload_spec=workload.spec, k8s=k8s, namespace=args.namespace
    )

    # Ensure there is enough space to scale
    # TODO: This need cluster role binding
    # validate_can_deployment_scale(k8s=k8s, deployment=deployment, replicas_to_add=1)
    logger.info("HPA cpu scaling guardrail success")
