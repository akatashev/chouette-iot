import json
import time
from unittest.mock import patch

import pytest
from redis import Redis, RedisError
from redis.client import Pipeline

import chouette_iot.storage.messages as msgs
from chouette_iot.metrics._metrics import WrappedMetric
from chouette_iot.storage import StorageActor


@pytest.fixture
def storage_actor_redis(monkeypatch):
    """
    Redis actor fixture.
    Since StorageActor actor is stateless, scope of the fixture is module.
    """
    monkeypatch.setenv("API_KEY", "whatever")
    monkeypatch.setenv("GLOBAL_TAGS", '["chouette-iot:est:chouette-iot"]')
    monkeypatch.setenv("METRICS_WRAPPER", "simple")
    monkeypatch.setenv("STORAGE_TYPE", "redis")
    actor_ref = StorageActor.get_instance()
    yield actor_ref
    actor_ref.stop()


def test_redis_gets_queue_sizes_correctly(storage_actor_redis, stored_raw_values):
    """
    StorageActor returns a correct number of messages in a queue.

    GIVEN: There are messages in a queue.
    WHEN: GetQueueSize message is set to StorageActor.
    THEN: This queue's size is returned.
    """
    message = msgs.GetQueueSize("metrics", wrapped=False)
    queue_size = storage_actor_redis.ask(message)
    assert queue_size == 5


def test_redis_gets_queue_sizes_exception(storage_actor_redis, stored_raw_values):
    """
    StorageActor returns -1 as a queue size in case of an exception.

    GIVEN: Storage raises an exception on GetQueueSize request.
    WHEN: GetQueueSize message is set to StorageActor.
    THEN: -1 is returned.
    """
    message = msgs.GetQueueSize("metrics", wrapped=False)
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        queue_size = storage_actor_redis.ask(message)
    assert queue_size == -1


def test_redis_gets_keys_correctly(storage_actor_redis, stored_raw_keys):
    """
    Redis returns a list of tuples (record_uid, timestamp) on CollectKeys.
    GIVEN: There are some keys in a corresponding set in Redis.
    WHEN: CollectKeys message is sent to StorageActor.
    THEN: It returns a list of tuples with these keys.
    """
    message = msgs.CollectKeys("metrics", wrapped=False)
    collected_keys = storage_actor_redis.ask(message)
    assert collected_keys == stored_raw_keys


def test_redis_gets_values_correctly(
    storage_actor_redis, metrics_keys, stored_raw_values
):
    """
    Redis returns a list of bytes-encoded record strings on CollectValues.
    GIVEN: There are raw records stored in a queue.
    AND: We have their keys.
    WHEN: CollectValues message is sent to StorageActor.
    THEN: It returns a list of bytes with these records.
    """
    keys = [key for key, ts in metrics_keys]
    expected_values = [value for key, value in stored_raw_values]
    message = msgs.CollectValues("metrics", keys, wrapped=False)
    collected_values = storage_actor_redis.ask(message)
    assert collected_values == expected_values


@pytest.mark.parametrize("redis_version", ["3.2.1", "5.0.5"])
def test_redis_stores_records_correctly(
    redis_version, redis_cleanup, post_test_actors_stop, monkeypatch
):
    """
    Redis stores records to a specified queue correctly.
    GIVEN: We have a correct record we need to store.
    WHEN: We send a StoreRecords to StorageActor.
    THEN: It returns True.
    AND: One record is being added to the queue keys.
    AND: The queue hash contains our metric under this key.
    """
    with patch.object(Redis, "info", return_value={"redis_version": redis_version}):
        monkeypatch.setenv("API_KEY", "whatever")
        monkeypatch.setenv("GLOBAL_TAGS", '["chouette-iot:est:chouette-iot"]')
        monkeypatch.setenv("METRICS_WRAPPER", "simple")
        storage_actor_redis = StorageActor.get_instance()
    metric = WrappedMetric(
        metric="important-metric",
        type="count",
        timestamp=3600,
        value=36.6,
        tags=["importance:high"],
    )
    message = msgs.StoreRecords("metrics", [metric], wrapped=True)
    result = storage_actor_redis.ask(message)
    assert result is True
    # Checks whether it was stored correctly:
    keys = storage_actor_redis.ask(msgs.CollectKeys("metrics", wrapped=True))
    assert len(keys) == 1
    key = keys.pop()[0]
    values = storage_actor_redis.ask(msgs.CollectValues("metrics", [key], wrapped=True))
    assert len(values) == 1
    value = json.loads(values.pop())
    assert value == metric.asdict()


def test_redis_drops_wrong_records_on_storing(storage_actor_redis, redis_cleanup):
    """
    Redis ignores records that it can't cast to dicts during storaging.
    GIVEN: I have a set of correct and incorrect records for storing.
    WHEN: We send a StoreRecords message with these records.
    THEN: It returns true.
    AND: Only valid records are stored.
    """
    metric_1 = WrappedMetric(
        metric="important-metric",
        type="count",
        timestamp=3600,
        value=36.6,
        tags=["importance:high"],
    )
    metric_2 = WrappedMetric(
        metric="important-metric",
        type="count",
        timestamp=7200,
        value=99.9,
        tags=["importance:high"],
    )
    metrics = [metric_1, "dsgsadgag", metric_2, storage_actor_redis]
    message = msgs.StoreRecords("metrics", metrics, wrapped=True)
    result = storage_actor_redis.ask(message)
    assert result is True
    # Checks whether it was stored correctly:
    keys_records = storage_actor_redis.ask(msgs.CollectKeys("metrics", wrapped=True))
    assert len(keys_records) == 2
    keys = [key for key, ts in keys_records]
    values = storage_actor_redis.ask(msgs.CollectValues("metrics", keys, wrapped=True))
    assert len(values) == 2
    values_dicts = list(map(json.loads, values))
    for metric in (metric_1, metric_2):
        assert metric.asdict() in values_dicts


def test_redis_deletes_records_correctly(
    storage_actor_redis, stored_raw_keys, stored_raw_values
):
    """
    Redis removes records fom a specified queue correctly.
    GIVEN: There are records stored to the queue.
    WHEN: We send a DeleteRecords message to StorageActor specifying some keys.
    THEN: It returns True.
    AND: These keys disappear from the keys set.
    AND: These values disappear from the values hash.
    """
    keys_records = [key for key, ts in stored_raw_keys]
    message = msgs.DeleteRecords("metrics", keys_records[0:-1], wrapped=False)
    result = storage_actor_redis.ask(message)
    assert result is True
    # Checks that it deleted records correctly:
    keys = storage_actor_redis.ask(msgs.CollectKeys("metrics", wrapped=False))
    assert len(keys) == 1
    assert stored_raw_keys[-1] in keys
    values = storage_actor_redis.ask(
        msgs.CollectValues("metrics", keys_records, wrapped=False)
    )
    assert len(values) == 1
    assert values[0] == stored_raw_values[4][1]


def test_redis_cleans_outdated_metrics_correctly(storage_actor_redis, redis_cleanup):
    """
    Redis cleans up outdated wrapped records correctly.
    Too old metrics are rejected by Datadog, so they should be cleaned up
    before dispatching. CleanupOutdatedRecords cleans all the records older
    than specified 'ttl' value.
    GIVEN: There are a bunch metrics in a queue and some of them are outdated.
    WHEN: CleanupOutdatedRecords message is sent to StorageActor.
    THEN: It returns True.
    AND: All outdated metrics are deleted from the queue.
    """
    now = int(time.time())
    metric_1 = WrappedMetric(metric="a", type="b", value=10)
    metric_2 = WrappedMetric(metric="a", type="b", value=20, timestamp=now - 7200)
    metrics = [
        WrappedMetric(metric="a", type="b", value=30, timestamp=now - 28000),
        WrappedMetric(metric="a", type="b", value=40, timestamp=now - 14401),
        metric_1,
        metric_2,
    ]
    storage_actor_redis.ask(msgs.StoreRecords("metrics", metrics, wrapped=True))
    # Test preparation finished.
    message = msgs.CleanupOutdatedRecords("metrics", ttl=14400, wrapped=True)
    result = storage_actor_redis.ask(message)
    assert result is True
    # Checks that cleanup was correct.
    keys_and_ts = storage_actor_redis.ask(msgs.CollectKeys("metrics", wrapped=True))
    assert len(keys_and_ts) == 2
    keys = [key for key, ts in keys_and_ts]
    values = storage_actor_redis.ask(msgs.CollectValues("metrics", keys, wrapped=True))
    assert len(values) == 2
    values_dicts = list(map(json.loads, values))
    for metric in (metric_1, metric_2):
        assert metric.asdict() in values_dicts


@pytest.mark.parametrize(
    "message",
    [
        msgs.CollectKeys("metrics", wrapped=False),
        msgs.CollectValues("metrics", [b"key"], wrapped=True),
    ],
)
def test_redis_returns_nil_on_failed_collections(storage_actor_redis, message):
    """
    StorageActor returns an empty list on failed Collection messages:
    GIVEN: There is a message of a Collection type.
    BUT: Redis instance is not ready to handle it.
    WHEN: Collection is requested from StorageActor.
    THEN: StorageActor returns an empty list.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        collection_result = storage_actor_redis.ask(message)
    assert collection_result == []


def test_redis_returns_empty_list_on_empty_collect_value(
    storage_actor_redis, redis_cleanup
):
    """
    StorageActor returns an empty list on a CollectValue request with empty
    keys list.
    GIVEN: There is a CollectValues message with an empty list of keys.
    WHEN: Collection is requested from StorageActor.
    THEN: StorageActor returns an empty list.
    """
    message = msgs.CollectValues("metrics", [], wrapped=True)
    collection_result = storage_actor_redis.ask(message)
    assert collection_result == []


@pytest.mark.parametrize(
    "message",
    [
        msgs.DeleteRecords("metrics", [b"key"], wrapped=False),
        msgs.StoreRecords(
            "metrics",
            [WrappedMetric(metric="a", type="b", timestamp=1, value=1)],
            wrapped=True,
        ),
        msgs.CleanupOutdatedRecords("metrics", ttl=14400, wrapped=True),
    ],
)
def test_redis_returns_false_on_failed_actions(storage_actor_redis, message):
    """
    StorageActor returns False when actions were not executed successfully.
    GIVEN: There is an action message.
    BUT: Redis instance isn't ready to process it, or it's malformed.
    WHEN: Execution is requested from StorageActor.
    THEN: StorageActor returns False.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        with patch.object(Pipeline, "execute", side_effect=RedisError):
            execution_result = storage_actor_redis.ask(message)
    assert execution_result is False


@pytest.mark.parametrize(
    "message",
    [
        msgs.DeleteRecords("metrics", [], wrapped=False),
        msgs.StoreRecords("metrics", [], wrapped=True),
    ],
)
def test_redis_returns_true_on_empty_actions(storage_actor_redis, message):
    """
    StorageActor returns True on empty action messages.
    There are two action messages with lists of arguments:
    DeleteRecords and StoreRecords.
    GIVEN: There is an empty action message.
    WHEN: Execution is requested.
    THEN: True is returned regardless of a Redis instance state.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        execution_result = storage_actor_redis.ask(message)
    assert execution_result is True


def test_redis_returns_none_on_unexpected_message(storage_actor_redis):
    """
    StorageActor returns None on unexpected message.
    GIVEN: There is an unexpected message.
    WHEN: It is sent to StorageActor.
    THEN: It returns None.
    """
    execution_result = storage_actor_redis.ask("Are you bored of being so in-memory?")
    assert execution_result is None
