import json
import os
import time
from unittest.mock import patch

import pytest
from redis import Redis, RedisError
from redis.client import Pipeline

import chouette.storages.messages as msgs
from chouette.metrics import WrappedMetric
from chouette.storages import RedisStorage


@pytest.fixture(scope="module")
def redis_client():
    """
    Redis client fixture.
    """
    redis_host = os.environ.get("REDIS_HOST", "redis")
    redis_port = os.environ.get("REDIS_PORT", "6379")
    return Redis(host=redis_host, port=redis_port)


@pytest.fixture(scope="module")
def redis_actor():
    """
    Redis actor fixture.

    Since RedisStorage actor is stateless, scope of the fixture is module.
    """
    actor_ref = RedisStorage.get_instance()
    return actor_ref


@pytest.fixture
def stored_keys(redis_client, raw_metrics_keys):
    """
    Fixture that stores dummy raw metrics keys to Redis.

    Before and after every test queue set is being cleaned up.
    """
    redis_client.delete("chouette:raw:metrics.keys")
    for key, ts in raw_metrics_keys:
        redis_client.zadd("chouette:raw:metrics.keys", {key: ts})
    yield raw_metrics_keys
    redis_client.delete("chouette:raw:metrics.keys")


@pytest.fixture
def stored_values(redis_client, raw_metrics_values):
    """
    Fixture that stores dummy raw metrics values to Redis.

    Before and after every test queue hash is being cleaned up.
    """
    redis_client.delete("chouette:raw:metrics.values")
    for key, message in raw_metrics_values:
        redis_client.hset("chouette:raw:metrics.values", key, message)
    yield raw_metrics_values
    redis_client.delete("chouette:raw:metrics.values")


@pytest.fixture
def redis_cleanup(redis_client):
    """
    Fixture that wraps a test with Redis cleanups.
    """
    redis_client.flushall()
    yield True
    redis_client.flushall()


def test_redis_gets_keys_correctly(redis_actor, stored_keys):
    """
    Redis returns a list of tuples (record_uid, timestamp) on CollectKeys.

    GIVEN: There are some keys in a corresponding set in Redis.
    WHEN: CollectKeys message is sent to RedisStorage.
    THEN: It returns a list of tuples with these keys.
    """
    message = msgs.CollectKeys("metrics", wrapped=False)
    collected_keys = redis_actor.ask(message)
    assert collected_keys == stored_keys


def test_redis_gets_values_correctly(redis_actor, raw_metrics_keys, stored_values):
    """
    Redis returns a list of bytes-encoded record strings on CollectValues.

    GIVEN: There are raw records stored in a queue.
    AND: We have their keys.
    WHEN: CollectValues message is sent to RedisStorage.
    THEN: It returns a list of bytes with these records.
    """
    keys = [key for key, ts in raw_metrics_keys]
    expected_values = [value for key, value in stored_values]
    message = msgs.CollectValues("metrics", keys, wrapped=False)
    collected_values = redis_actor.ask(message)
    assert collected_values == expected_values


def test_redis_stores_records_correctly(redis_actor, redis_cleanup):
    """
    Redis stores records to a specified queue correctly.

    GIVEN: We have a correct record we need to store.
    WHEN: We send a StoreRecords to RedisStorage.
    THEN: It returns True.
    AND: One record is being added to the queue keys.
    AND: The queue hash contains our metric under this key.
    """
    metric = WrappedMetric("important-metric", "count", 3600, 10.5, ["importance:high"])
    message = msgs.StoreRecords("metrics", [metric], wrapped=True)
    result = redis_actor.ask(message)
    assert result is True
    # Checks whether it was stored correctly:
    keys = redis_actor.ask(msgs.CollectKeys("metrics", wrapped=True))
    assert len(keys) == 1
    key = keys.pop()[0]
    values = redis_actor.ask(msgs.CollectValues("metrics", [key], wrapped=True))
    assert len(values) == 1
    value = json.loads(values.pop())
    assert value == metric.asdict()


def test_redis_drops_wrong_records_on_storing(redis_actor, redis_cleanup):
    """
    Redis ignores records that it can't cast to dicts during storaging.

    GIVEN: I have a set of correct and incorrect records for storing.
    WHEN: We send a StoreRecords message with these records.
    THEN: It returns true.
    AND: Only valid records are stored.
    """
    metric_1 = WrappedMetric(
        "important-metric", "count", 3600, 36.6, ["importance:high"]
    )
    metric_2 = WrappedMetric(
        "important-metric", "count", 7200, 99.9, ["importance:high"]
    )
    metrics = [metric_1, "dsgsadgag", metric_2, redis_actor]
    message = msgs.StoreRecords("metrics", metrics, wrapped=True)
    result = redis_actor.ask(message)
    assert result is True
    # Checks whether it was stored correctly:
    keys_records = redis_actor.ask(msgs.CollectKeys("metrics", wrapped=True))
    assert len(keys_records) == 2
    keys = [key for key, ts in keys_records]
    values = redis_actor.ask(msgs.CollectValues("metrics", keys, wrapped=True))
    assert len(values) == 2
    values_dicts = list(map(json.loads, values))
    for metric in (metric_1, metric_2):
        assert metric.asdict() in values_dicts


def test_redis_deletes_records_correctly(redis_actor, stored_keys, stored_values):
    """
    Redis removes records fom a specified queue correctly.

    GIVEN: There are records stored to the queue.
    WHEN: We send a DeleteRecords message to RedisStorage specifying some keys.
    THEN: It returns True.
    AND: These keys disappear from the keys set.
    AND: These values disappear from the values hash.
    """
    keys_records = [key for key, ts in stored_keys]
    message = msgs.DeleteRecords("metrics", keys_records[0:-1], wrapped=False)
    result = redis_actor.ask(message)
    assert result is True
    # Checks that it deleted records correctly:
    keys = redis_actor.ask(msgs.CollectKeys("metrics", wrapped=False))
    assert len(keys) == 1
    assert stored_keys[-1] in keys
    values = redis_actor.ask(msgs.CollectValues("metrics", keys_records, wrapped=False))
    assert len(values) == 1
    assert values[0] == stored_values[4][1]


def test_redis_cleans_outdated_metrics_correctly(redis_actor, redis_cleanup):
    """
    Redis cleans up outdated wrapped records correctly.

    Too old metrics are rejected by DataDog, so they should be cleaned up
    before dispatching. CleanupOutdatedRecords cleans all the records older
    than specified 'ttl' value.

    GIVEN: There are a bunch metrics in a queue and some of them are outdated.
    WHEN: CleanupOutdatedRecords message is sent to RedisStorage.
    THEN: It returns True.
    AND: All outdated metrics are deleted from the queue.
    """
    now = int(time.time())
    metric_1 = WrappedMetric("a", "b", 10, now)
    metric_2 = WrappedMetric("a", "b", 20, now - 7200)
    metrics = [
        WrappedMetric("a", "b", 30, now - 28000),
        WrappedMetric("a", "b", 40, now - 14401),
        metric_1,
        metric_2,
    ]
    redis_actor.ask(msgs.StoreRecords("metrics", metrics, wrapped=True))
    # Test preparation finished.
    message = msgs.CleanupOutdatedRecords("metrics", ttl=14400)
    result = redis_actor.ask(message)
    assert result is True
    # Checks that cleanup was correct.
    keys_and_ts = redis_actor.ask(msgs.CollectKeys("metrics", wrapped=True))
    assert len(keys_and_ts) == 2
    keys = [key for key, ts in keys_and_ts]
    values = redis_actor.ask(msgs.CollectValues("metrics", keys, wrapped=True))
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
def test_redis_returns_nil_on_failed_collections(redis_actor, message):
    """
    RedisStorage returns an empty list on failed Collection messages:

    GIVEN: There is a message of a CollectValues or CollectKeys type.
    BUT: Redis instance is not ready to handle it.
    WHEN: Collection is requested from RedisStorage.
    THEN: RedisStorage returns an empty list.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        collection_result = redis_actor.ask(message)
    assert collection_result == []


@pytest.mark.parametrize(
    "message",
    [
        msgs.DeleteRecords("metrics", [b"key"], wrapped=False),
        msgs.StoreRecords("metrics", [b"not-a-valid-object"], wrapped=True),
        msgs.StoreRecords("metrics", [WrappedMetric("a", "b", 1, 1)], wrapped=True),
        msgs.CleanupOutdatedRecords("metrics", 14400),
    ],
)
def test_redis_returns_false_on_failed_actions(redis_actor, message):
    """
    RedisStorage returns False when actions were not executed successfully.

    GIVEN: There is an action message.
    BUT: Redis instance isn't ready to process it, or it's malformed.
    WHEN: Execution is requested from RedisStorage.
    THEN: RedisStorage returns False.
    """
    with patch.object(Pipeline, "execute", side_effect=RedisError):
        execution_result = redis_actor.ask(message)
    assert execution_result is False


@pytest.mark.parametrize(
    "message",
    [
        msgs.DeleteRecords("metrics", [], wrapped=False),
        msgs.StoreRecords("metrics", [], wrapped=True),
    ],
)
def test_redis_returns_true_on_empty_actions(redis_actor, message):
    """
    RedisStorage returns True on empty action messages.

    There are two action messages with lists of arguments:
    DeleteRecords and StoreRecords.

    GIVEN: There is an empty action message.
    WHEN: Execution is requested.
    THEN: True is returned regardless of a Redis instance state.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        execution_result = redis_actor.ask(message)
    assert execution_result is True


def test_redis_returns_none_on_unexpected_message(redis_actor):
    """
    RedisStorage returns None on unexpected message.

    GIVEN: There is an unexpected message.
    WHEN: It is sent to RedisStorage.
    THEN: It returns None.
    """
    execution_result = redis_actor.ask("Are you bored of being so in-memory?")
    assert execution_result is None