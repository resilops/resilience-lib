from kubernetes import client, config as k8config
from kubernetes.config.config_exception import ConfigException


class KubernetesClient:

    def __init__(self) -> None:
        self._load_config()

    @staticmethod
    def _load_config() -> None:
        try:
            k8config.load_incluster_config()
        except ConfigException:
            k8config.load_kube_config()

    @property
    def api(self):
        return client.ApiClient()

    @property
    def v1_api(self) -> client.CoreV1Api:
        return client.CoreV1Api(self.api)

    @property
    def apps(self) -> client.AppsV1Api:
        return client.AppsV1Api(self.api)

    @property
    def autoscaling(self) -> client.AutoscalingV2Api:
        return client.AutoscalingV2Api(self.api)

    @property
    def policy(self) -> client.PolicyV1Api:
        return client.PolicyV1Api(self.api)

    @property
    def custom(self) -> client.CustomObjectsApi:
        return client.CustomObjectsApi()
