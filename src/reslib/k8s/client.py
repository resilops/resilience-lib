import asyncio
from typing import Optional

from kubernetes import client, config as k8config
from kubernetes.config.config_exception import ConfigException


class KubernetesClient:
    """
    Thin wrapper around the Kubernetes Python client.

    - Loads kubeconfig or in-cluster config on init.
    - Lazily initializes and caches API clients.
    - Provides async helpers that offload blocking calls to threads.
    """

    def __init__(self) -> None:
        self._load_config()
        self._api: Optional[client.ApiClient] = None
        self._v1_api: Optional[client.CoreV1Api] = None
        self._apps_api: Optional[client.AppsV1Api] = None
        self._autoscaling_api: Optional[client.AutoscalingV2Api] = None
        self._policy_api: Optional[client.PolicyV1Api] = None
        self._custom_api: Optional[client.CustomObjectsApi] = None

    @staticmethod
    def _load_config() -> None:
        try:
            k8config.load_incluster_config()
        except ConfigException:
            k8config.load_kube_config()

    @property
    def api(self):
        if self._api is None:
            self._api = client.ApiClient()
        return self._api

    @staticmethod
    def new_api() -> client.ApiClient:
        """Create a fresh ApiClient instance for one-off calls (e.g. exec/stream)."""
        return client.ApiClient()

    @property
    def v1_api(self) -> client.CoreV1Api:
        if self._v1_api is None:
            self._v1_api = client.CoreV1Api(self.api)
        return self._v1_api

    @property
    def apps(self) -> client.AppsV1Api:
        if self._apps_api is None:
            self._apps_api = client.AppsV1Api(self.api)
        return self._apps_api

    @property
    def autoscaling(self) -> client.AutoscalingV2Api:
        if self._autoscaling_api is None:
            self._autoscaling_api = client.AutoscalingV2Api(self.api)
        return self._autoscaling_api

    @property
    def policy(self) -> client.PolicyV1Api:
        if self._policy_api is None:
            self._policy_api = client.PolicyV1Api(self.api)
        return self._policy_api

    @property
    def custom(self) -> client.CustomObjectsApi:
        if self._custom_api is None:
            self._custom_api = client.CustomObjectsApi(self.api)
        return self._custom_api

    async def read_namespaced_deployment(self, *, name: str, namespace: str):
        return await asyncio.to_thread(
            self.apps.read_namespaced_deployment, name=name, namespace=namespace
        )

    async def read_namespaced_pod(self, *, name: str, namespace: str):
        return await asyncio.to_thread(
            self.v1_api.read_namespaced_pod, name=name, namespace=namespace
        )

    async def list_namespaced_pod(self, *, namespace: str, label_selector: str):
        return await asyncio.to_thread(
            self.v1_api.list_namespaced_pod,
            namespace=namespace,
            label_selector=label_selector,  # noqa
        )

    async def delete_namespaced_pod(self, *, name: str, namespace: str, body):
        return await asyncio.to_thread(
            self.v1_api.delete_namespaced_pod,
            name=name,
            namespace=namespace,
            body=body,
        )

    async def read_namespaced_horizontal_pod_autoscaler(
        self, *, name: str, namespace: str
    ):
        return await asyncio.to_thread(
            self.autoscaling.read_namespaced_horizontal_pod_autoscaler,
            name=name,
            namespace=namespace,
        )

    async def list_namespaced_horizontal_pod_autoscaler(self, *, namespace: str):
        return await asyncio.to_thread(
            self.autoscaling.list_namespaced_horizontal_pod_autoscaler,
            namespace=namespace,
        )

    async def list_namespaced_pod_disruption_budget(self, *, namespace: str):
        return await asyncio.to_thread(
            self.policy.list_namespaced_pod_disruption_budget,
            namespace=namespace,
        )

    async def get_namespaced_custom_object(
        self,
        *,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
    ):
        return await asyncio.to_thread(
            self.custom.get_namespaced_custom_object,
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            name=name,
        )

    async def list_namespaced_deployment(self, *, namespace: str):
        return await asyncio.to_thread(
            self.apps.list_namespaced_deployment,
            namespace=namespace,
        )
