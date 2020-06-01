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
    redis_client.hset(
        "dramatiq:fake.msgs",
        mapping={
            b"key-1": b'{"metric": "metric1"}',
            b"key-2": b'{"metric": "metric2"}',
        },
    )
    return "dramatiq:fake.msgs"


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
    AND: This metrics tags contain this Dramatiq queue name.
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
