import pytest
from pykka import ActorRegistry

from chouette.metrics import WrappedMetric
from chouette.metrics.plugins import DramatiqCollector
from chouette.metrics.plugins._dramatiq_collector import DramatiqCollectorPlugin
from chouette.metrics.plugins.messages import StatsRequest, StatsResponse


@pytest.fixture
def dramatiq_ref():
    """
    DramatiqCollector ActorRef fixture.
    """
    ActorRegistry.stop_all()
    actor_ref = DramatiqCollector.get_instance()
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
    DramatiqCollector sends back a valid StatsResponse message.

    GIVEN: There is a single Dramatiq queue with 2 messages.
    WHEN: DramatiqCollector receives a StatsRequest message.
    THEN: It sends back a StatsResponse message.
    AND: Its sender is DramatiqCollector.
    AND: Its stats property is an Iterator over WrappedMetric objects.
    AND: It contains a single metric.
    AND: This metrics tags contain this Dramatiq queue name stripped of
         "dramatiq:" prefix and ".msgs" postfix.
    AND: Its value is 2.
    """
    dramatiq_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    assert isinstance(response, StatsResponse)
    assert response.producer == "DramatiqCollector"
    stats = list(response.stats)
    assert all(isinstance(elem, WrappedMetric) for elem in stats)
    metric = stats.pop()
    assert metric.tags == [f"queue:{dramatiq_queue}"]
    assert metric.value == 2


def test_dramatiq_collector_wraps_sizes():
    """
    DramatiqCollector wraps hashes sizes into WrappedMetrics.

    GIVEN: I have a list of queues names and sizes.
    WHEN: This list is passed to the 'wrap_queues_sizes' method.
    THEN: An Iterator over WrappedMetrics is returned.
    AND: It contains metrics with correct tags.
    """
    sizes = [("queue1", 10), ("queue2", 5), ("queue3", 14)]
    metrics = list(DramatiqCollectorPlugin.wrap_queues_sizes(sizes))
    assert all(isinstance(metric, WrappedMetric) for metric in metrics)
    tags = sorted(metric.tags for metric in metrics)
    assert tags == sorted([["queue:queue1"], ["queue:queue2"], ["queue:queue3"]])


def test_dramatiq_collector_does_not_crash_on_stopped_sender(test_actor, dramatiq_ref):
    """
    DramatiqCollector doesn't crash on stopped sender

    GIVEN: I have a working DramatiqCollector actor.
    WHEN: Some actor sends a StatsRequest and stops before it gets a response.
    THEN: HostStatsCollector doesn't crash.
    """
    test_actor.stop()
    dramatiq_ref.ask(StatsRequest(test_actor))
    assert dramatiq_ref.is_alive()


def test_dramatiq_collector_does_not_crash_on_wrong_sender(dramatiq_ref):
    """
    DramatiqCollector doesn't crash on wrong sender.

    GIVEN: I have a working DramatiqCollector actor.
    WHEN: Some actor sends a StatsRequest with some gibberish as a sender.
    THEN: HostStatsCollector doesn't crash.
    """
    dramatiq_ref.ask(StatsRequest("not an actor"))
    assert dramatiq_ref.is_alive()
