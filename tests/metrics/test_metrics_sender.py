import time
from unittest.mock import patch

import pytest
from pykka import ActorRegistry
from redis import RedisError
from redis.client import Pipeline

from chouette_iot.metrics import MetricsSender
from chouette_iot.metrics import WrappedMetric
from chouette_iot.storages.messages import StoreRecords, CollectKeys


@pytest.fixture
def sender_actor(monkeypatch, mocked_http):
    """
    MetricsSender Actor fixture.
    """
    monkeypatch.setenv("host", "test_host")
    actor_ref = MetricsSender.get_instance()
    yield actor_ref
    ActorRegistry.stop_all()


@pytest.fixture
def sender_proxy(sender_actor):
    """
    MetricsSender Actor Proxy fixture for actor methods testing.
    """
    return sender_actor.proxy()


@pytest.fixture
def stored_wrapped_keys(redis_client, metrics_keys):
    """
    Fixture that stores dummy wrapped metrics keys to Redis.

    Before and after every test queue set is being cleaned up.
    """
    redis_client.delete("chouette:metrics:wrapped.keys")
    for key, ts in metrics_keys:
        redis_client.zadd("chouette:metrics:wrapped.keys", {key: ts})
    yield metrics_keys
    redis_client.delete("chouette:metrics:wrapped.keys")


@pytest.fixture
def expected_metrics(redis_client, sender_proxy, redis_cleanup):
    """
    Fixture that stores dummy wrapped metrics values to Redis and
    returns expected metrics for `collect_records` method tests.

    Before and after every test queue hash is being cleaned up.
    """
    metrics = [
        WrappedMetric(
            metric="metric-1",
            type="count",
            value=10,
            timestamp=time.time() - 300,
            tags=["my:tag"],
        ),
        WrappedMetric(metric="metric-2", type="gauge", value=20, timestamp=time.time()),
    ]
    sender_proxy.redis.get().ask(StoreRecords("metrics", metrics, wrapped=True))
    # Adding a "metric" that is not JSON parseable:
    redis_client.zadd(
        "chouette:metrics:wrapped.keys", {"wrong-metric-uid": time.time()}
    )
    redis_client.hset(
        "chouette:metrics:wrapped.values", b"wrong-metric-uid", b"So wrong!"
    )
    # Generate expected metrics by adding global tags to tags fields.
    global_tags = sender_proxy.tags.get()
    dicts = [metric.asdict() for metric in metrics]
    for metric in dicts:
        metric["tags"] += global_tags
        metric["host"] = "test_host"
    yield dicts


def test_sender_returns_true(sender_actor, expected_metrics):
    """
    MetricsSender returns True after successful dispatch and
    cleans up dispatched metrics.

    GIVEN: There are metrics in the wrapped metrics queue.
    AND: Everything is fine.
    WHEN: MetricsSender receives a message.
    THEN: It returns True.
    AND: Dispatched metrics are removed from the queue.
    """
    result = sender_actor.ask("dispatch")
    assert result is True
    sender_proxy = sender_actor.proxy()
    keys = sender_proxy.collect_keys("metrics").get()
    assert not keys


def test_sender_returns_true_on_no_keys(sender_actor, redis_client, redis_cleanup):
    """
    MetricsSender returns True if there is nothing to dispatch.

    GIVEN: There are no metrics in the wrapped metrics queue.
    WHEN: MetricsSender receives a message.
    THEN: It returns True, because its work is finished successfully.
    """
    result = sender_actor.ask("dispatch")
    assert result is True


@pytest.mark.parametrize("api_key", ["authfail", "exc"])
def test_sender_returns_false_on_dispatch_problems(
    monkeypatch, expected_metrics, api_key, mocked_http
):
    """
    MetricsSender returns False on dispatch problems.

    GIVEN: There are metrics in the wrapped metrics queue.
    AND: For some reason request to Datadog doesn't return 202 Accepted.
    WHEN: MetricsSender receives a message.
    THEN: It returns False, because metrics were not dispatched.
    AND: Metrics are not deleted from the queue.
    """
    monkeypatch.setenv("API_KEY", api_key)
    ActorRegistry.stop_all()
    sender_actor = MetricsSender.get_instance()
    result = sender_actor.ask("dispatch")
    assert result is False
    sender_proxy = sender_actor.proxy()
    keys = sender_proxy.collect_keys("metrics").get()
    values = sender_proxy.collect_records(keys, "metrics").get()
    assert values == expected_metrics


def test_sender_returns_false_on_redis_problems(
    monkeypatch, expected_metrics, sender_actor
):
    """
    MetricsSender returns False on Redis problems during metrics cleanup.

    GIVEN: There are metrics in the wrapped metrics queue.
    AND: Datadog works fine and returns 202.
    AND: On deletion attempt Redis returns RedisError
    WHEN: MetricsSender receives a message.
    THEN: It returns False, because metrics were not deleted.
    """
    with patch.object(Pipeline, "execute", side_effect=RedisError):
        result = sender_actor.ask("dispatch")
    assert result is False
    sender_proxy = sender_actor.proxy()
    keys = sender_proxy.collect_keys("metrics").get()
    values = sender_proxy.collect_records(keys, "metrics").get()
    assert values == expected_metrics


def test_sender_collect_keys_returns_list_of_keys(sender_proxy, stored_wrapped_keys):
    """
    MetricsSender's `collect_keys` method returns a list of keys.

    GIVEN: There are keys in a wrapped metrics set.
    WHEN: Method `collect_keys` is called with amount 3.
    THEN: It returns a list of bytes with 3 earliest keys.
    """
    expected_keys = [key for key, ts in stored_wrapped_keys]
    result = sender_proxy.collect_keys("metrics").get()
    assert result == expected_keys[0:3]


def test_sender_collect_records_returns_list_of_processed_metrics(
    sender_proxy, expected_metrics
):
    """
    MetricsSender's `collect_records` methods returns a list of JSON
    strings containing stored metrics whose tags are updated with global tags.

    GIVEN: There are records in the wrapped metrics queue.
    AND: One of the records is not a valid metric.
    WHEN: Method `collect_records` is called with their keys.
    THEN: It returns a list of dicts.
    AND: These dicts represent previously stored metrics.
    AND: Every metric has global tags added to its `tags` property.
    AND: Invalid record is ignored.
    """
    keys = sender_proxy.collect_keys("metrics").get()
    assert len(keys) == 3
    dicts = sender_proxy.collect_records(keys, "metrics").get()
    assert dicts == expected_metrics
    assert len(dicts) == 2


def test_sender_dispatch_to_datadog(sender_proxy, expected_metrics):
    """
    MetricsSender `dispatch_to_datadog` returns True on 202 Accepted.

    GIVEN: It's possible to connect to Datadog and api_key is correct.
    WHEN: `dispatch_to_datadog` method is called for valid metrics.
    THEN: True is returned when 202 Accepted response is received.
    """
    result = sender_proxy.dispatch_to_datadog(expected_metrics).get()
    assert result is True


@pytest.mark.parametrize("api_key", ["authfail", "exc"])
def test_sender_dispatch_to_datadog_problem(monkeypatch, expected_metrics, api_key):
    """
    MetricsSender `dispatch_to_datadog` returns False on 403 Auth error or
    Requests exception.

    GIVEN: It's possible to connect to Datadog and api_key is incorrect.
    WHEN: `dispatch_to_datadog` method is called for valid metrics.
    THEN: False is returned if 202 Accepted wasn't returned.
    """
    monkeypatch.setenv("API_KEY", api_key)
    ActorRegistry.stop_all()
    sender_proxy = MetricsSender.get_instance().proxy()
    result = sender_proxy.dispatch_to_datadog(expected_metrics).get()
    assert result is False


@pytest.mark.parametrize("send_self_metrics", [False, True])
def test_sender_sends_self_metrics(monkeypatch, expected_metrics, send_self_metrics):
    """
    MetricsSender stores dispatched metrics number and size if
    `send_self_metrics` option is set to True.

    Scenario 1:
    GIVEN: Option `send_self_metrics` is set to True.
    AND: There are more metrics in the queue than Chouette sends (in this
         scenario this "extra" metrics is an invalid metrics but normally
         it happens when there is too many metrics for one batch).
    WHEN: `dispatch_to_datadog` method is called and executed successfully.
    THEN: 2 `chouette.metrics.dispatched` raw metrics are stored to Redis.
    AND: `choette.queue.metrics` metric is stored to Redis.

    Scenario 2:
    GIVEN: Option `send_self_metrics` is set to False.
    WHEN: `dispatch_to_datadog` method is called and executed successfully.
    THEN: No raw metrics are stored to Redis.
    """
    monkeypatch.setenv("SEND_SELF_METRICS", str(send_self_metrics))
    ActorRegistry.stop_all()
    sender_proxy = MetricsSender.get_instance().proxy()
    sender_proxy.dispatch_to_datadog(expected_metrics).get()
    redis = sender_proxy.redis.get()
    # Sleep due to async ChouetteClient nature:
    time.sleep(0.1)
    keys = redis.ask(CollectKeys("metrics", wrapped=False))
    assert (len(keys) == 3) is send_self_metrics
