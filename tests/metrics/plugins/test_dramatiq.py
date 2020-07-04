import pytest
from pykka import ActorRegistry

from chouette_iot.metrics.plugins._dramatiq_collector import (
    DramatiqCollector,
    DramatiqCollectorPlugin,
    WrappedMetric,
)
from chouette_iot.metrics.plugins.messages import StatsRequest, StatsResponse
from redis import Redis, RedisError
from unittest.mock import patch


@pytest.fixture
def dramatiq_ref():
    """
    DramatiqCollectorPlugin ActorRef fixture.
    """
    ActorRegistry.stop_all()
    actor_ref = DramatiqCollectorPlugin.get_instance()
    yield actor_ref
    ActorRegistry.stop_all()


@pytest.fixture
def dramatiq_queue(redis_client, redis_cleanup):
    """
    Fake Dramatiq queue fixture.
    """
    redis_client.hset("dramatiq:fake.msgs", b"key-1", b'{"metric": "metric1"}')
    redis_client.hset("dramatiq:fake.msgs", b"key-2", b'{"metric": "metric2"}')
    return "fake"


def test_dramatiq_collector_handles_stats_requests(
    dramatiq_ref, test_actor, dramatiq_queue
):
    """
    DramatiqCollectorPlugin sends back a valid StatsResponse message.

    GIVEN: There is a single Dramatiq queue with 2 messages.
    WHEN: DramatiqCollectorPlugin receives a StatsRequest message.
    THEN: It sends back a StatsResponse message.
    AND: Its sender is DramatiqCollectorPlugin.
    AND: Its stats property is an Iterator over WrappedMetric objects.
    AND: It contains a single metric.
    AND: This metrics tags contain this Dramatiq queue name stripped of
         "dramatiq:" prefix and ".msgs" postfix.
    AND: Its value is 2.
    """
    dramatiq_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    assert isinstance(response, StatsResponse)
    assert response.producer == "DramatiqCollectorPlugin"
    stats = list(response.stats)
    assert all(isinstance(elem, WrappedMetric) for elem in stats)
    metric = stats.pop()
    assert metric.tags == [f"queue:{dramatiq_queue}"]
    assert metric.value == 2


def test_dramatiq_collector_reads_queues_names(dramatiq_queue):
    """
    DramatiqCollector returns a correct list of Dramatiq queues names.
    
    GIVEN: There is a Dramatiq queue in Redis.
    WHEN: DramatiqCollector's _get_queues_names method is called.
    THEN: A list of queues names is returned.
    """
    queues_names = DramatiqCollector._get_queues_names("dramatiq:*.msgs")
    assert queues_names == [b"dramatiq:fake.msgs"]


def test_dramatiq_collector_reads_queues_names_redis_error():
    """
    DramatiqCollector returns an empty list of queues on RedisError.

    GIVEN: Redis is ready to raise a RedisError.
    WHEN: DramatiqCollector's _get_queues_names method is called.
    THEN: An empty list of queues names is returned.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        queue_names = DramatiqCollector._get_queues_names("dramatiq:*.msgs")
    assert queue_names == []


def test_dramatiq_collector_returns_queues_sizes(dramatiq_queue):
    """
    DramatiqCollector returns a correct list of queues sizes.

    GIVEN: There is a Dramatiq queue in Redis.
    WHEN: _get_queues_sizes method is called with a list of valid queues names.
    THEN: A list of tuples with correct queues names and theirs sizes is returned.
    """
    queues_sizes = DramatiqCollector._get_queues_sizes([b"dramatiq:fake.msgs"])
    assert queues_sizes == [("dramatiq:fake.msgs", 2)]


def test_dramatiq_collector_returns_queue_sizes_redis_error(dramatiq_queue):
    """
    DramatiqCollector returns an empty list of queues sizes on RedisError.

    GIVEN: Redis is ready to raise a RedisError.
    WHEN: _get_queues_sizes method is called with a list of valid queues names.
    THEN: An empty list is returned.
    """
    with patch.object(Redis, "execute_command", side_effect=RedisError):
        queue_sizes = DramatiqCollector._get_queues_sizes([b"dramatiq:fake.msgs"])
    assert queue_sizes == []


def test_dramatiq_collector_wraps_sizes():
    """
    DramatiqCollectorPlugin wraps hashes sizes into WrappedMetrics.

    GIVEN: I have a list of queues names and sizes.
    WHEN: This list is passed to the 'wrap_queues_sizes' method.
    THEN: An Iterator over WrappedMetrics is returned.
    AND: It contains metrics with correct tags.
    """
    sizes = [("queue1", 10), ("queue2", 5), ("queue3", 14)]
    metrics = list(DramatiqCollector._wrap_queues_sizes(sizes))
    assert all(isinstance(metric, WrappedMetric) for metric in metrics)
    tags = sorted(metric.tags for metric in metrics)
    assert tags == sorted([["queue:queue1"], ["queue:queue2"], ["queue:queue3"]])


def test_dramatiq_collector_does_not_crash_on_stopped_sender(test_actor, dramatiq_ref):
    """
    DramatiqCollectorPlugin doesn't crash on stopped sender

    GIVEN: I have a working DramatiqCollectorPlugin actor.
    WHEN: Some actor sends a StatsRequest and stops before it gets a response.
    THEN: HostStatsCollector doesn't crash.
    """
    test_actor.stop()
    dramatiq_ref.ask(StatsRequest(test_actor))
    assert dramatiq_ref.is_alive()


def test_dramatiq_collector_does_not_crash_on_wrong_sender(dramatiq_ref):
    """
    DramatiqCollectorPlugin doesn't crash on wrong sender.

    GIVEN: I have a working DramatiqCollectorPlugin actor.
    WHEN: Some actor sends a StatsRequest with some gibberish as a sender.
    THEN: HostStatsCollector doesn't crash.
    """
    dramatiq_ref.ask(StatsRequest("not an actor"))
    assert dramatiq_ref.is_alive()
