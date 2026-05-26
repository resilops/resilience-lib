import logging
from typing import Dict

from reslib import helpers as h
from reslib.core.context import get_context, set_context
from reslib.k8s.client import KubernetesClient
from reslib.k8s.schema import WorkloadState
from reslib.schemas.scenario import ResiliencyScenario

logger = logging.getLogger(__name__)

ROLLING_RESTART_ANNOTATION = "resilopshq.com/restartedAt"


async def perform_rolling_restart(**kwargs) -> Dict:
    """
    Trigger a Kubernetes Deployment rolling restart.

    The restart is initiated by patching the pod template metadata annotation.
    Kubernetes then creates a new ReplicaSet using the current image,
    configuration, secrets, and runtime dependencies.
    """
    logger.info("Starting rolling restart")
    workload: WorkloadState = get_context("workload")
    scenario: ResiliencyScenario = get_context("scenario")
    namespace = scenario.template.namespace
    workload_name = scenario.template.workload
    restarted_at = h.utc_now_iso()

    k8s = KubernetesClient()
    deployment = await k8s.patch_namespaced_deployment(
        name=workload_name,
        namespace=namespace,
        body={
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            ROLLING_RESTART_ANNOTATION: restarted_at,
                        }
                    }
                }
            }
        },
    )

    restart_generation = deployment.metadata.generation
    set_context("rolling_restart_started_at", restarted_at)
    set_context("rolling_restart_generation", restart_generation)

    logger.info("Rolling restart triggered")
    return {
        "result": "rolling_restart_started",
        "reason": ("Deployment pod template was patched to trigger a rolling restart."),
        "observed": {
            "workload": workload.spec.name,
            "namespace": namespace,
            "restart_annotation": ROLLING_RESTART_ANNOTATION,
            "rolling_restart_started_at": restarted_at,
            "rolling_restart_generation": restart_generation,
        },
    }
