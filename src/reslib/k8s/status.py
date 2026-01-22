
from typing import List
from kubernetes.client import V1Deployment
from kubernetes import config as k8config


def current_cluster_name() -> str:
    """
    Get current active cluster name from the config

    Returns:
        Name of the cluster (string)
    """
    _, active_context = k8config.list_kube_config_contexts()
    return active_context.get("context", {}).get("cluster")


def is_deployment_condition_true(deployment: V1Deployment, condition_type: str) -> bool:
    """
    Check if a deployment satisfies a given condition.

    Args:
        deployment: Kubernetes deployment object.
        condition_type: Condition to check (e.g., "Available", "Progressing").

    Returns:
        True if condition is True, else False.
    """
    conditions: List = deployment.status.conditions or []
    for condition in conditions:
        if condition.type == condition_type and condition.status == "True":
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
