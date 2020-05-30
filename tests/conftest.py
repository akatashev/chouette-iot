import os

import pytest
from pykka import ActorRegistry
from redis import Redis

from chouette._singleton_actor import SingletonActor


@pytest.fixture(scope="session")
def redis_client():
    """
    Redis client fixture.
    """
    redis_host = os.environ.get("REDIS_HOST", "redis")
    redis_port = os.environ.get("REDIS_PORT", "6379")
    return Redis(host=redis_host, port=redis_port)


@pytest.fixture
def post_test_actors_stop():
    """
    Stops all started actors after a test is finished.
    """
    yield True
    ActorRegistry.stop_all()


@pytest.fixture(scope="session")
def test_actor_class():
    class TestActor(SingletonActor):
        def __init__(self):
            super().__init__()
            self.messages = []

        def on_receive(self, message):
            if message == "messages":
                return self.messages
            if message == "count":
                return len(self.messages)
            self.messages.append(message)
            return None

    return TestActor


@pytest.fixture
def test_actor(test_actor_class):
    """
    Test ActorRef fixture.
    """
    ref = test_actor_class.start()
    yield ref
    ref.stop()


@pytest.fixture(scope="session")
def metrics_keys():
    return [
        (b"metric-uuid-1", 10),  # Keys group 1
        (b"metric-uuid-2", 12),  # Keys group 1
        (b"metric-uuid-3", 23),  # Keys group 2
        (b"metric-uuid-4", 31),  # Keys group 3
        (b"metric-uuid-5", 34),  # Keys group 3
    ]


@pytest.fixture(scope="session")
def raw_metrics_values():
    return [
        (
            b"metric-uuid-1",
            b'{"metric": "metric-1", "type": "count", "timestamp": 10, "value": 10}',
        ),
        (
            b"metric-uuid-2",
            b'{"metric": "metric-2", "type": "gauge", "timestamp": 12, "value": 7}',
        ),
        (
            b"metric-uuid-3",
            b'{"metric": "metric-3", "type": "gauge", "timestamp": 23, "value": 12, "tags": {"very": "important"}}',
        ),
        (
            b"metric-uuid-4",
            b'{"metric": "metric-4", "type": "gauge", "timestamp": 31, "value": 10}',
        ),
        (
            b"metric-uuid-5",
            b'{"metric": "metric-4", "type": "gauge", "timestamp": 31, "value": 6}',
        ),
    ]


@pytest.fixture
def stored_raw_keys(redis_client, metrics_keys):
    """
    Fixture that stores dummy raw metrics keys to Redis.

    Before and after every test queue set is being cleaned up.
    """
    redis_client.delete("chouette:raw:metrics.keys")
    for key, ts in metrics_keys:
        redis_client.zadd("chouette:raw:metrics.keys", {key: ts})
    yield metrics_keys
    redis_client.delete("chouette:raw:metrics.keys")


@pytest.fixture
def stored_raw_values(redis_client, raw_metrics_values):
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
