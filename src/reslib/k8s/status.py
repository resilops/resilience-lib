from typing import List

from kubernetes import config as k8config
from kubernetes.client import V1Deployment

from reslib.constants import (
    DEPLOYMENT_CONDITION_AVAILABLE,
    DEPLOYMENT_CONDITION_PROGRESSING,
    DEPLOYMENT_STATUS_MIN_RS_AVAILABLE,
    DEPLOYMENT_STATUS_PROGRESS_DEADLINE,
    DEPLOYMENT_STATUS_RS_AVAILABLE,
)


def current_cluster_name() -> str:
    """
    Get current active cluster name from the config

    Returns:
        Name of the cluster (string)
    """
    _, active_context = k8config.list_kube_config_contexts()
    return active_context.get("context", {}).get("cluster")


def is_deployment_in_progress(deployment: V1Deployment) -> bool:
    """
    Determine if a Deployment is currently rolling out.

    A deployment is considered "in progress" if:
      - Progressing=True
      - AND the reason is not "NewReplicaSetAvailable"

    Args:
        deployment: V1Deployment object.

    Returns:
        True if rollout is in progress, False otherwise.
    """
    conditions: List = deployment.status.conditions or []

    for cond in conditions:
        if (
            cond.type == DEPLOYMENT_CONDITION_PROGRESSING
            and cond.status == "True"
            and cond.reason != DEPLOYMENT_STATUS_RS_AVAILABLE
        ):
            return True

    return False


def is_deployment_available(deployment: V1Deployment) -> bool:
    """
    Determine if a Deployment is currently available.

    Args:
        deployment: V1Deployment object.

    Returns:
        True if serving traffic, False otherwise.
    """
    conditions: List = deployment.status.conditions or []
    for cond in conditions:
        if (
            cond.type == DEPLOYMENT_CONDITION_AVAILABLE
            and cond.status == "True"
            and cond.reason == DEPLOYMENT_STATUS_MIN_RS_AVAILABLE
        ):
            return True
    return False


def is_deployment_faulty(deployment: V1Deployment) -> bool:
    """
    Determine whether a Deployment is in a failed state.

    A Deployment is considered faulty if Kubernetes reports that
    the rollout has failed due to exceeding the progress deadline.

    Args:
        deployment: V1Deployment object.

    Returns:
        True if the Deployment is faulty, False otherwise.
    """
    conditions: List = deployment.status.conditions or []

    for cond in conditions:
        if (
            cond.type == DEPLOYMENT_CONDITION_PROGRESSING
            and cond.status == "False"
            and cond.reason == DEPLOYMENT_STATUS_PROGRESS_DEADLINE
        ):
            return True

    return False


def ready_replicas(deployment: V1Deployment) -> int:
    """
    Get the number of ready replicas for a deployment.

    Args:
        deployment: Kubernetes deployment object.

    Returns:
        Number of ready replicas.
    """
    return deployment.status.ready_replicas or 0
