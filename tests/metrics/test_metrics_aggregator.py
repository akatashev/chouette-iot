import json
from unittest.mock import patch

import pytest
from pykka import ActorRegistry

from chouette_iot.metrics import MetricsAggregator, RawMetric
from chouette_iot.storages import RedisStorage
from chouette_iot.storages.messages import StoreRecords, CollectKeys, CollectValues


@pytest.fixture
def aggregator_ref(monkeypatch, redis_client):
    ActorRegistry.stop_all()
    monkeypatch.setenv("API_KEY", "whatever")
    monkeypatch.setenv("GLOBAL_TAGS", '["chouette-iot:est:chouette-iot"]')
    monkeypatch.setenv("METRICS_WRAPPER", "simple")
    actor_ref = MetricsAggregator.start()
    yield actor_ref
    ActorRegistry.stop_all()


@pytest.fixture
def redis_with_raw_metrics(redis_cleanup):
    metrics = [
        RawMetric(metric="metric-test", type="count", value=1, tags={"test": "test"}),
        RawMetric(metric="metric-test", type="count", value=2, tags={"test": "test"}),
    ]
    redis = RedisStorage.get_instance()
    redis.ask(StoreRecords("metrics", metrics, wrapped=False))
    return redis


def test_aggregator_without_merger_drops_messages(monkeypatch):
    """
    When MetricsAggregator doesn't have a proper MetricsWrapper, it
    doesn't process a message.

    GIVEN: Option METRICS_WRAPPER contains some unknown wrapper name.
    WHEN: MetricsAggregator is started and it receives a message.
    THEN: It returns True on this message.
    AND: Its metrics_wrapper parameter is None.
    AND: It doesn't try to reach Redis to get metrics keys.
    """
    monkeypatch.setenv("API_KEY", "whatever")
    monkeypatch.setenv("GLOBAL_TAGS", '["chouette-iot:est:chouette-iot"]')
    monkeypatch.setenv("METRICS_WRAPPER", "none")
    aggregator_ref = MetricsAggregator.start()
    with patch.object(RedisStorage, "_collect_keys") as collect_keys:
        result = aggregator_ref.ask("aggregate")
    assert result is True
    proxy = aggregator_ref.proxy()
    assert proxy.metrics_wrapper.get() is None
    collect_keys.assert_not_called()


def test_aggregator_outdated_values(aggregator_ref, stored_raw_values, stored_raw_keys):
    """
    Aggregator cleans up outdated metrics before collecting keys.

    GIVEN: There are outdated metrics in a raw metrics queue.
    WHEN: MetricsAggregator receives a message.
    THEN: It cleans up all the outdated metrics.
    AND: Its collect_keys method is executed but it returns an empty list.
    AND: Its collect_values method is not executed.
    """
    with patch.object(RedisStorage, "_collect_keys") as collect_keys:
        with patch.object(RedisStorage, "_collect_values") as collect_values:
            result = aggregator_ref.ask("aggregate")
    assert result
    collect_keys.assert_called()
    collect_values.assert_not_called()


def test_aggregator_non_empty_queue(aggregator_ref, redis_with_raw_metrics):
    """
    Aggregator gets actual raw metrics, merges them and stores to a wrapped
    metrics queue.

    GIVEN: There are 2 raw metrics of the same type in a raw metrics queue.
    WHEN: MetricsAggregator receives a message.
    THEN: When processing is finished, MetricsAggregator returns True.
    AND: One WrappedMetric appears in a wrapped metrics queue.
    AND: Raw metrics are cleaned up from the raw metrics queue.
    """
    result = aggregator_ref.ask("aggregate")
    assert result
    stored_keys = redis_with_raw_metrics.ask(CollectKeys("metrics", wrapped=True))
    stored_metrics = redis_with_raw_metrics.ask(
        CollectValues("metrics", [key for key, _ in stored_keys], wrapped=True)
    )
    assert len(stored_metrics) == 1
    stored_metric = json.loads(stored_metrics.pop())
    assert stored_metric["metric"] == "metric-test"
    assert stored_metric["type"] == "count"
    assert stored_metric["tags"] == ["test:test"]
    assert stored_metric["points"][0][1] == 3
    raw_keys = redis_with_raw_metrics.ask(CollectKeys("metrics", wrapped=False))
    assert not raw_keys


def test_aggregator_storing_failed(aggregator_ref, redis_with_raw_metrics):
    """
    When aggregator can't store WrappedMetrics to a wrapped queue, it doesn't try to
    delete

    GIVEN: There are 2 raw metrics of the same type in a raw metrics queue.
    AND: For some reason a storage can't store data to a wrapped metrics queue.
    WHEN: MetricsAggregator receives a message.
    THEN: It returns False.
    AND: Raw metrics are not being cleaned up.
    """

    with patch.object(RedisStorage, "_store_records", return_value=False):
        with patch.object(RedisStorage, "_delete_records") as delete_records:
            result = aggregator_ref.ask("aggregate")
    assert result is False
    stored_keys = redis_with_raw_metrics.ask(CollectKeys("metrics", wrapped=False))
    stored_metrics = redis_with_raw_metrics.ask(
        CollectValues("metrics", [keys for keys, _ in stored_keys], wrapped=False)
    )
    assert len(stored_metrics) == 2
    delete_records.assert_not_called()


def test_aggregator_cleanup_failed(aggregator_ref, redis_with_raw_metrics):
    """
    When aggregator can't cleanup RawMetrics it returns False.

    GIVEN: There are 2 raw metrics of the same type in a raw metrics queue.
    AND: For some reason a storage can't delete date from its queue.
    WHEN: MetricsAggregator receives a message.
    THEN: It returns False.
    """
    with patch.object(RedisStorage, "_delete_records", return_value=False):
        result = aggregator_ref.ask("aggregate")
    assert result is False
