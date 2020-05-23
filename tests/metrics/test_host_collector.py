"""
HostStatsCollector plugin tests.

NB: `ask` is used here due to its blocking nature because for testing we need
to be sure, that processing finished when we check the results.

Normal interaction between Collectors and plugins MUST be non-blocking.
`ask` pattern would return None.
"""
from unittest.mock import patch

import pytest

from chouette.metrics import WrappedMetric
from chouette.metrics.plugins import HostStatsCollector
from chouette.metrics.plugins.messages import StatsRequest, StatsResponse


@pytest.fixture
def test_actor(test_actor_class):
    """
    Test ActorRef fixture.
    """
    ref = test_actor_class.start()
    yield ref
    ref.stop()


@pytest.fixture(scope="module")
def collector_ref():
    """
    HostStatsCollector ActorRef fixture.
    """
    ref = HostStatsCollector.get_instance()
    yield ref
    ref.stop()


def test_host_collector_returns_stats_response(test_actor, collector_ref):
    """
    HostStatsCollector sends an iterator over WrappedMetrics to a
    StatsRequest sender.

    GIVEN: I have a working HostStatsCollector actor.
    WHEN: Some actor sends a StatsRequest message with correct sender.
    THEN: After a short period of time it receives a response.
    AND: This response type is StatsResponse.
    AND: Its 'producer' property is HostStatsCollector.
    AND: Its `stats` property is an iterator over WrappedMetrics.
    """
    collector_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    assert isinstance(response, StatsResponse)
    assert response.producer == "HostStatsCollector"
    stats = response.stats
    assert all(map(lambda elem: isinstance(elem, WrappedMetric), stats))


@patch("psutil.cpu_percent")
@pytest.mark.parametrize(
    "cpu_perc_value, metric_must_exist", [(0.0, False), (10.5, True)]
)
def test_host_collector_ignores_cpu_percentage_zero(
    cpu_perc, test_actor, collector_ref, cpu_perc_value, metric_must_exist
):
    """
    HostStatsCollector doesn't send CPU percentage metric with value 0.0.

    GIVEN: psutil method cpu_percent was never run before and returns 0.0.
    WHEN: Some actor sends a StatsRequest message with correct sender.
    THEN: After a short period of time it receives a response.
    BUT: There is no `host.cpu.percentage` metric in this response.
    """
    cpu_perc.return_value = cpu_perc_value
    collector_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    metric_exists = any(stat.metric == "host.cpu.percentage" for stat in response.stats)
    assert metric_exists == metric_must_exist


def test_host_collector_does_not_crash_on_wrong_sender(test_actor, collector_ref):
    """
    HostStatsCollector doesn't crash on wrong sender.

    GIVEN: I have a working HostStatsCollector actor.
    WHEN: Some actor sends a StatsRequest with some gibberish as a sender.
    THEN: HostStatsCollector doesn't crash.
    """
    collector_ref.ask(StatsRequest(test_actor))
    assert collector_ref.is_alive()
