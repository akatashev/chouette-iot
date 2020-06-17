from unittest.mock import patch

import pytest
from pykka import ActorRegistry

from chouette_iot.metrics import MetricsCollector
from chouette_iot.metrics.plugins import PluginsFactory
from chouette_iot.metrics.plugins.messages import StatsResponse
from chouette_iot.storages import RedisStorage
from chouette_iot.storages.messages import StoreRecords


@pytest.fixture
def collector_ref(monkeypatch):
    """
    MetricsCollector actor fixture.
    """
    ActorRegistry.stop_all()
    monkeypatch.setenv("API_KEY", "whatever")
    monkeypatch.setenv("GLOBAL_TAGS", '["chouette-iot:est:chouette-iot"]')
    monkeypatch.setenv("COLLECTOR_PLUGINS", '["ice-cream", "berries"]')
    actor_ref = MetricsCollector.start()
    yield actor_ref
    ActorRegistry.stop_all()


def test_collector_reads_plugin_list(monkeypatch, post_test_actors_stop):
    """
    MetricsCollector successfully reads the list of plugins from env vars.

    GIVEN: There is some list of plugins configured in envvars.
    WHEN: MetricsCollector actor is started.
    THEN: It loads them and stores into its `plugins` property.
    """
    monkeypatch.setenv("API_KEY", "whatever")
    monkeypatch.setenv("GLOBAL_TAGS", '["chouette-iot:est:chouette-iot"]')
    monkeypatch.setenv("COLLECTOR_PLUGINS", '["ice-cream", "berries"]')
    collector_ref = MetricsCollector.start()
    loaded_plugins = collector_ref.proxy().plugins.get()
    assert set(loaded_plugins) == {"ice-cream", "berries"}


def test_collector_sends_requests_to_plugins(collector_ref, test_actor):
    """
    MetricsCollector sends StatsRequest messages to its plugin on any message
    that is not a StatsResponse message.

    GIVEN: We have a MetricCollector with 2 plugins configured.
    WHEN: MetricsCollector receives a message.
    THEN: It sends every of its plugins one StatsRequest message.
    AND: It puts itself into sender field of these messages.

    NB: In this test plugins generation is mocked, so TestActor "acts" as both
        plugins and receives two messages.
    """
    with patch.object(PluginsFactory, "get_plugin", return_value=test_actor):
        collector_ref.ask("collect")
    messages = test_actor.ask("messages")
    assert len(messages) == len(collector_ref.proxy().plugins.get())
    assert all(message.sender == collector_ref for message in messages)


def test_collector_stores_metrics_to_redis(collector_ref, test_actor):
    """
    MetricCollector sends a StoreRecords message to RedisStorage when it
    receives a StatsResponse message.

    GIVEN: There is a StatsResponse message with some stats.
    WHEN: MetricCollector receives this message.
    THEN: It sends a StoreRecords message to RedisStorage.
    AND: Its wrapped property is True.
    AND: Its records property contains stats from the original message.
    AND: Its data_type property is 'metrics'.
    """
    stats = ["stat1", "stat2", "stat3"]
    with patch.object(RedisStorage, "get_instance", return_value=test_actor):
        collector_ref.ask(StatsResponse("Unit-Test", stats))
    messages = test_actor.ask("messages")
    assert len(messages) == 1
    message = messages.pop()
    assert isinstance(message, StoreRecords)
    assert message.wrapped is True
    assert message.records == stats
    assert message.data_type == "metrics"
