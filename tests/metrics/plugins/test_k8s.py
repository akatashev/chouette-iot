import json
from unittest.mock import patch

import pytest
from pykka import ActorRegistry

from chouette.metrics import WrappedMetric
from chouette.metrics.plugins import K8sCollector
from chouette.metrics.plugins._k8s_collector import K8sCollectorPlugin
from chouette.metrics.plugins.messages import StatsRequest, StatsResponse


@pytest.fixture
def k8s_ref(monkeypatch):
    """
    K8sCollector ActorRef fixture.
    """
    monkeypatch.setenv("K8S_STATS_SERVICE_IP", "10.1.18.1")
    monkeypatch.setenv("K8S_CERT_PATH", "client.crt")
    monkeypatch.setenv("K8S_KEY_PATH", "client.key")
    monkeypatch.setenv("K8S_METRICS", '["node", "pods"]')
    actor_ref = K8sCollector.get_instance()
    yield actor_ref
    ActorRegistry.stop_all()


def test_k8s_plugin_handles_stats_request(mocked_http, k8s_ref, test_actor):
    """
    K8sCollector is able to handle StatsResponse requests:

    GIVEN: K8s stats server is up and running.
    AND: Our cert and key are correct.
    WHEN: K8sCollector receives an StatsRequest message.
    THEN: It sends back a StatsResponse message.
    AND: This response contains WrappedMetric objects.
    """
    k8s_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    assert isinstance(response, StatsResponse)
    assert response.producer == "K8sCollector"
    stats = response.stats
    assert stats
    assert all(isinstance(elem, WrappedMetric) for elem in stats)


def test_k8s_plugin_gets_raw_metrics(mocked_http, k8s_stats_response):
    """
    K8sCollectorPlugin returns a dict that contains correct K8s stats.

    GIVEN: K8s stats server is up and running.
    AND: Our cert and key are correct.
    WHEN: K8sCollectorPlugin method `gets_raw_metrics` is used.
    THEN: It returns a dict that contains correct K8S stats.
    """
    url = "https://10.1.18.1:10250/stats/summary"
    cert = ("client.crt", "client.key")
    result = K8sCollectorPlugin._get_raw_metrics(url, cert)
    assert result == json.loads(k8s_stats_response)


@pytest.mark.parametrize("postfix", ["notjson", "exc", "wrongcreds"])
def test_k8s_plugin_gets_raw_metrics_empty(postfix, mocked_http):
    """
    K8sCollectorPlugin returns a empty dict in case of problems.

    GIVEN: K8s stats server won't return a JSON response for some reason
           (Unauthorized, exceptions, internal errors, etc).
    WHEN: K8sCollectorPlugin method `_get_raw_metrics` is used.
    THEN: It returns an empty dict.
    """
    url = f"https://10.1.18.1:10250/stats/{postfix}"
    cert = ("client.crt", "client.key")
    result = K8sCollectorPlugin._get_raw_metrics(url, cert)
    assert result == {}


def test_k8s_plugin_collects_stats(k8s_stats_response):
    """
    K8sCollectorPlugin returns an iterator over WrappedMetrics.

    GIVEN: K8s stats server is up and running.
    AND: Our cert and key are correct.
    WHEN: K8sCollectorPlugin method `collect_stats` is used.
    THEN: It returns an iterator over WrappedMetrics.
    AND: It contains metrics with data about both a node and its pods.
    """
    with patch.object(
        K8sCollectorPlugin,
        "_get_raw_metrics",
        return_value=json.loads(k8s_stats_response),
    ):
        stats = list(
            K8sCollectorPlugin.collect_stats("a", ("b", "c"), ["node", "pods"])
        )
    assert stats
    assert all(isinstance(stat, WrappedMetric) for stat in stats)
    tags = set(sum([stat.tags for stat in stats], []))
    assert tags == {
        "node_name:nano",
        "pod_name:coredns-588fd544bf-8btq7",
        "namespace:kube-system",
    }


def test_k8s_plugin_collects_stats_empty(k8s_stats_response):
    """
    K8sCollectorPlugin returns an empty iterator in case of problems.

    GIVEN: '_get_raw_metrics' returns an empty dict for some reason.
    WHEN: K8sCollectorPlugin method `collect_stats` is used.
    THEN: It returns an empty iterator.
    """
    with patch.object(K8sCollectorPlugin, "_get_raw_metrics", return_value={}):
        stats = list(
            K8sCollectorPlugin.collect_stats("a", ("b", "c"), ["node", "pods"])
        )
    assert stats == []


def test_k8s_plugin_parses_node_stats(k8s_stats_response):
    """
    K8sCollectorPlugin parses node stats from a raw metrics response.

    GIVEN: There is a valid dict with K8s stats service response.
    WHEN: This dict is passed to the `_parse_node_metrics` method.
    THEN: It returns an iterator over WrappedMetric objects.
    AND: All of these metrics have tags ["node_name:<node_name>"].
    """
    k8s_response = json.loads(k8s_stats_response)
    node_stats = list(K8sCollectorPlugin._parse_node_metrics(k8s_response))
    assert all(isinstance(stat, WrappedMetric) for stat in node_stats)
    assert all(stat.tags == ["node_name:nano"] for stat in node_stats)


def test_k8s_plugin_parses_node_stats_wrong_dict():
    """
    K8sCollectorPlugin node parsing doesn't crash on an empty raw metrics
    response.

    GIVEN: There is a empty dict as a K8s service stats response.
    WHEN: This dict is passed to the `_parse_node_metrics` method.
    THEN: It returns an empty iterator.
    """
    node_stats = list(K8sCollectorPlugin._parse_node_metrics({}))
    assert node_stats == []


def test_k8s_plugin_parses_pods_stats(k8s_stats_response):
    """
    K8sCollectorPlugin parses pods stats from a raw metrics response.

    GIVEN: There is a valid dict with K8s stats service response.
    WHEN: This dict is passed to the `_parse_pods_metrics` method.
    THEN: It returns an iterator over WrappedMetric objects.
    AND: There are metrics for every pod on the node.
    """
    k8s_response = json.loads(k8s_stats_response)
    pods_stats = list(K8sCollectorPlugin._parse_pods_metrics(k8s_response))
    assert all(isinstance(stat, WrappedMetric) for stat in pods_stats)
    assert all(
        stat.tags
        == sorted(["pod_name:coredns-588fd544bf-8btq7", "namespace:kube-system"])
        for stat in pods_stats
    )


def test_k8s_plugin_parses_pod_metrics_no_pod_ref():
    """
    K8sCollectorPlugin pod parsing doesn't crash on an empty raw metrics
    response.

    GIVEN: There is a empty dict as a K8s service stats response.
    WHEN: This dict is passed to the `_parse_pod_metrics` method.
    THEN: It returns an empty iterator.
    """
    pod_stats = list(K8sCollectorPlugin._parse_pod_metrics({}))
    assert pod_stats == []


def test_k8s_plugin_does_not_crash_on_stopped_sender(test_actor, k8s_ref):
    """
    K8sCollectorPlugin doesn't crash on stopped sender

    GIVEN: I have a working K8sCollectorPlugin actor.
    WHEN: Some actor sends a StatsRequest and stops before it gets a response.
    THEN: HostStatsCollector doesn't crash.
    """
    test_actor.stop()
    k8s_ref.ask(StatsRequest(test_actor))
    assert k8s_ref.is_alive()


def test_k8s_plugin_does_not_crash_on_wrong_sender(k8s_ref):
    """
    K8sCollectorPlugin doesn't crash on wrong sender.

    GIVEN: I have a working K8sCollectorPlugin actor.
    WHEN: Some actor sends a StatsRequest with some gibberish as a sender.
    THEN: HostStatsCollector doesn't crash.
    """
    k8s_ref.ask(StatsRequest("not an actor"))
    assert k8s_ref.is_alive()
