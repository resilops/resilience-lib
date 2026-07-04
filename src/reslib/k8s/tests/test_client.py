from unittest.mock import patch

import pytest
from kubernetes.config.config_exception import ConfigException

from reslib.k8s.client import KubernetesClient


def test_load_config_falls_back_to_kube_config():
    with (
        patch("reslib.k8s.client.k8config.load_incluster_config") as load_incluster,
        patch("reslib.k8s.client.k8config.load_kube_config") as load_kube,
    ):
        load_incluster.side_effect = ConfigException("missing")

        KubernetesClient._load_config()

    load_incluster.assert_called_once_with()
    load_kube.assert_called_once_with()


def test_client_lazily_initializes_core_api():
    with patch.object(KubernetesClient, "_load_config"):
        client = KubernetesClient()

    with (
        patch("reslib.k8s.client.client.ApiClient", return_value="api-client"),
        patch("reslib.k8s.client.client.CoreV1Api", return_value="core-v1") as core_api,
    ):
        first = client.v1_api
        second = client.v1_api

    assert first == "core-v1"
    assert second == "core-v1"
    core_api.assert_called_once_with("api-client")


@pytest.mark.parametrize(
    ("property_name", "factory_path", "cache_attr", "factory_result"),
    [
        ("apps", "reslib.k8s.client.client.AppsV1Api", "_apps_api", "apps-api"),
        (
            "autoscaling",
            "reslib.k8s.client.client.AutoscalingV2Api",
            "_autoscaling_api",
            "autoscaling-api",
        ),
        ("policy", "reslib.k8s.client.client.PolicyV1Api", "_policy_api", "policy-api"),
        (
            "discovery",
            "reslib.k8s.client.client.DiscoveryV1Api",
            "_discovery_api",
            "discovery-api",
        ),
        (
            "custom",
            "reslib.k8s.client.client.CustomObjectsApi",
            "_custom_api",
            "custom-api",
        ),
    ],
)
def test_client_lazily_initializes_other_api_properties(
    property_name, factory_path, cache_attr, factory_result
):
    with patch.object(KubernetesClient, "_load_config"):
        client = KubernetesClient()

    with (
        patch("reslib.k8s.client.client.ApiClient", return_value="api-client"),
        patch(factory_path, return_value=factory_result) as factory,
    ):
        first = getattr(client, property_name)
        second = getattr(client, property_name)

    assert first == factory_result
    assert second == factory_result
    assert getattr(client, cache_attr) == factory_result
    factory.assert_called_once_with("api-client")


def test_new_helpers_create_fresh_clients():
    with (
        patch("reslib.k8s.client.client.ApiClient", return_value="api-client"),
        patch("reslib.k8s.client.client.CoreV1Api", return_value="core-v1") as core_api,
        patch("reslib.k8s.client.watch.Watch", return_value="watcher") as watch_factory,
    ):
        assert KubernetesClient.new_api() == "api-client"
        assert KubernetesClient.new_v1_api() == "core-v1"
        assert KubernetesClient.new_watch() == "watcher"

    core_api.assert_called_once_with("api-client")
    watch_factory.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "cache_attr", "target_method", "kwargs"),
    [
        (
            "read_namespaced_deployment",
            "_apps_api",
            "read_namespaced_deployment",
            {"name": "checkout-api", "namespace": "default"},
        ),
        (
            "patch_namespaced_deployment",
            "_apps_api",
            "patch_namespaced_deployment",
            {"name": "checkout-api", "namespace": "default", "body": {"spec": {}}},
        ),
        (
            "read_namespaced_pod",
            "_v1_api",
            "read_namespaced_pod",
            {"name": "checkout-pod", "namespace": "default"},
        ),
        (
            "patch_namespaced_pod",
            "_v1_api",
            "patch_namespaced_pod",
            {"name": "checkout-pod", "namespace": "default", "body": {"metadata": {}}},
        ),
        (
            "list_namespaced_pod",
            "_v1_api",
            "list_namespaced_pod",
            {"namespace": "default", "label_selector": "app=checkout"},
        ),
        (
            "delete_namespaced_pod",
            "_v1_api",
            "delete_namespaced_pod",
            {"name": "checkout-pod", "namespace": "default", "body": {"grace": 0}},
        ),
        (
            "create_namespaced_pod_eviction",
            "_v1_api",
            "create_namespaced_pod_eviction",
            {
                "name": "checkout-pod",
                "namespace": "default",
                "body": {"deleteOptions": {}},
            },
        ),
        (
            "read_namespaced_horizontal_pod_autoscaler",
            "_autoscaling_api",
            "read_namespaced_horizontal_pod_autoscaler",
            {"name": "checkout-api", "namespace": "default"},
        ),
        (
            "list_namespaced_horizontal_pod_autoscaler",
            "_autoscaling_api",
            "list_namespaced_horizontal_pod_autoscaler",
            {"namespace": "default"},
        ),
        (
            "list_namespaced_pod_disruption_budget",
            "_policy_api",
            "list_namespaced_pod_disruption_budget",
            {"namespace": "default"},
        ),
        (
            "get_namespaced_custom_object",
            "_custom_api",
            "get_namespaced_custom_object",
            {
                "group": "metrics.k8s.io",
                "version": "v1beta1",
                "namespace": "default",
                "plural": "pods",
                "name": "checkout-pod",
            },
        ),
        (
            "list_namespaced_deployment",
            "_apps_api",
            "list_namespaced_deployment",
            {"namespace": "default"},
        ),
        (
            "list_namespaced_service",
            "_v1_api",
            "list_namespaced_service",
            {"namespace": "default"},
        ),
        (
            "read_namespaced_service",
            "_v1_api",
            "read_namespaced_service",
            {"name": "checkout-svc", "namespace": "default"},
        ),
        (
            "list_namespaced_endpoint_slice",
            "_discovery_api",
            "list_namespaced_endpoint_slice",
            {
                "namespace": "default",
                "label_selector": "kubernetes.io/service-name=checkout",
            },
        ),
    ],
)
async def test_async_methods_delegate_to_asyncio_to_thread(
    method_name, cache_attr, target_method, kwargs
):
    captured = {}

    async def fake_to_thread(func, **passed_kwargs):
        captured["func"] = func
        captured["kwargs"] = passed_kwargs
        return "thread-result"

    api_stub = type("ApiStub", (), {target_method: object()})()

    with (
        patch.object(KubernetesClient, "_load_config"),
        patch("reslib.k8s.client.asyncio.to_thread", side_effect=fake_to_thread),
    ):
        client = KubernetesClient()
        setattr(client, cache_attr, api_stub)

        result = await getattr(client, method_name)(**kwargs)

    assert result == "thread-result"
    assert captured["func"] is getattr(api_stub, target_method)
    assert captured["kwargs"] == kwargs
