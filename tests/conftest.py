import pytest
from pykka import ActorRegistry
from redis import Redis
import os

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
        (b"metric-uuid-1", 10),
        (b"metric-uuid-2", 12),
        (b"metric-uuid-3", 23),
        (b"metric-uuid-4", 31),
        (b"metric-uuid-5", 34),
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
