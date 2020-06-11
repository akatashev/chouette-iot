"""
HostStatsCollector plugin tests.

NB: `ask` is used here due to its blocking nature because for testing we need
to be sure, that processing finished when we check the results.

Normal interaction between Collectors and plugins MUST be non-blocking.
`ask` pattern would return None.
"""
from unittest.mock import patch

import pytest
import psutil

from chouette_iot.metrics import WrappedMetric
from chouette_iot.metrics.plugins import HostStatsCollector
from chouette_iot.metrics.plugins.messages import StatsRequest, StatsResponse


@pytest.fixture
def collector_ref(monkeypatch):
    """
    HostStatsCollector ActorRef fixture.
    """
    monkeypatch.setenv(
        "HOST_COLLECTOR_METRICS", '["cpu", "la", "ram", "network", "fs"]'
    )
    ref = HostStatsCollector.get_instance()
    ref.stop()
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
    stats = list(response.stats)
    assert all(isinstance(elem, WrappedMetric) for elem in stats)


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
    metric_exists = any(
        stat.metric == "Chouette.host.cpu.percentage" for stat in response.stats
    )
    assert metric_exists == metric_must_exist


def test_host_collector_does_not_crash_on_stopped_sender(test_actor, collector_ref):
    """
    HostStatsCollector doesn't crash on stopped sender

    GIVEN: I have a working HostStatsCollector actor.
    WHEN: Some actor sends a StatsRequest and stops before it gets a response.
    THEN: HostStatsCollector doesn't crash.
    """
    test_actor.stop()
    collector_ref.ask(StatsRequest(test_actor))
    assert collector_ref.is_alive()


def test_host_collector_does_not_crash_on_wrong_sender(collector_ref):
    """
    HostStatsCollector doesn't crash on wrong sender.

    GIVEN: I have a working HostStatsCollector actor.
    WHEN: Some actor sends a StatsRequest with some gibberish as a sender.
    THEN: HostStatsCollector doesn't crash.
    """
    collector_ref.ask(StatsRequest("not an actor"))
    assert collector_ref.is_alive()


@patch("psutil.cpu_percent")
def test_host_collector_can_collect_subset_of_metrics(
    cpu_perc, test_actor, monkeypatch
):
    """
    It's possible to specify what subset of metrics you want to collect.

    GIVEN: HOST_COLLECTOR_METRICS specify only cpu metrics.
    WHEN: HostStatsCollector receives a StatRequest.
    THEN: It collects and sends back only cpu metrics.
    """
    cpu_perc.return_value = 10.0
    monkeypatch.setenv("HOST_COLLECTOR_METRICS", '["cpu"]')
    collector_ref = HostStatsCollector.start()
    collector_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    stats = list(response.stats)
    non_cpu_metrics = [stat for stat in stats if "cpu" not in stat.metric]
    cpu_metrics = [stat for stat in stats if "cpu" in stat.metric]
    assert cpu_metrics
    assert not non_cpu_metrics


def test_host_collector_collects_ram_metrics(test_actor, collector_ref):
    """
    HostStatsCollector returns RAM metrics.

    GIVEN: 'ram' is specified in host_collector_metrics configuration.
    WHEN: HostStatsCollector receives a StatRequest.
    THEN: It collects and sends 2 `Chouette.host.memory` metrics along with
          other metrics (used and available memory).
    """
    collector_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    stats = list(response.stats)
    ram_metrics = [stat for stat in stats if "Chouette.host.memory" in stat.metric]
    assert len(ram_metrics) == 2


def test_host_collector_collects_network_metrics(test_actor, collector_ref):
    """
    HostStatsCollector returns network metrics.

    GIVEN: 'network' is specified in host_collector_metrics configuration.
    AND: We have a single network interface.
    WHEN: HostStatsCollector receives a StatRequest.
    THEN: It collects and sends 2 `Chouette.host.network` metrics along with
          other metrics (bytes.sent and bytes.recv).
    """
    collector_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    stats = list(response.stats)
    network_metrics = [stat for stat in stats if "Chouette.host.network" in stat.metric]
    assert len(network_metrics) == 2


def test_host_collector_collects_fs_metrics(test_actor, collector_ref):
    """
    HostStatsCollector returns fs metrics.

    GIVEN: 'fs' is specified in host_collector_metrics configuration.
    AND: We have a single device (even if it's mounted to numerous points).
    WHEN: HostStatsCollector receives a StatRequest.
    THEN: It collects and sends 2 "Chouette.host.fs" metrics along with other
          metrics (used and free space on device).
    """
    collector_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    stats = list(response.stats)
    fs_metrics = [stat for stat in stats if "Chouette.host.fs" in stat.metric]
    partitions = {partition.device for partition in psutil.disk_partitions()}
    # >= is here due to CircleCI fluctuations. Normally it should be ==
    assert len(fs_metrics) >= 2 * len(partitions)
