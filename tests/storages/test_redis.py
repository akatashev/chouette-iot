import os
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

    Before and after every test redis is being cleaned up.
    """
    redis_client.flushall()
    for key, ts in raw_metrics_keys:
        redis_client.zadd("chouette:raw:metrics.keys", {key: ts})
    yield raw_metrics_keys
    redis_client.flushall()


@pytest.fixture
def stored_values(redis_client, raw_metrics_values):
    """
    Fixture that stores dummy raw metrics values to Redis.

    Before and after every test redis is being cleaned up.
    """
    redis_client.flushall()
    for key, message in raw_metrics_values:
        redis_client.hset("chouette:raw:metrics.values", key, message)
    yield raw_metrics_values
    redis_client.flushall()


def test_redis_gets_keys_correctly(redis_actor, stored_keys):
    """
    Redis returns a list of tuples (record_uid, timestamp) on CollectKeys.

    GIVEN: There are some keys in a corresponding set in Redis.
    WHEN: CollectKeys request is sent to RedisStorage.
    THEN: It returns a list of tuples with these keys.
    """
    request = msgs.CollectKeys("metrics")
    collected_keys = redis_actor.ask(request)
    assert collected_keys == stored_keys


def test_redis_gets_values_correctly(redis_actor, raw_metrics_keys, stored_values):
    """
    Redis returns a list of bytes-encoded record strings on CollectValues.

    GIVEN: There are raw records stored in a queue.
    AND: We have their keys.
    WHEN: CollectValues request is sent to RedisStorage.
    THEN: It returns a list of bytes with these records.
    """
    keys = [key for key, ts in raw_metrics_keys]
    expected_values = [value for key, value in stored_values]
    request = msgs.CollectValues("metrics", keys)
    collected_values = redis_actor.ask(request)
    assert collected_values == expected_values


@pytest.mark.skip
def test_redis_stores_records_correctly(redis_actor):
    pass


@pytest.mark.skip
def test_redis_deletes_records_correctly(redis_actor):
    pass


@pytest.mark.skip
def test_redis_cleans_outdated_metrics_correctly(redis_actor):
    pass


@pytest.mark.parametrize(
    "message",
    [
        msgs.CollectKeys("metrics", wrapped=False),
        msgs.CollectValues("metrics", [b"key"], wrapped=True),
    ],
)
def test_redis_returns_nil_on_failed_collections(redis_actor, message):
    """
    RedisStorage returns an empty list on failed Collection requests:

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

    GIVEN: There is an action request.
    BUT: Redis instance isn't ready to process it, or it's malformed.
    WHEN: Execution is requested from RedisStorage.
    THEN: RedisStorage returns False.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
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
    RedisStorage returns True on empty action requests.

    There are two action requests with lists of arguments:
    DeleteRecords and StoreRecords.

    GIVEN: There is an empty action request.
    WHEN: Execution is requested.
    THEN: True is returned regardless of a Redis instance state.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        execution_result = redis_actor.ask(message)
    assert execution_result is True
