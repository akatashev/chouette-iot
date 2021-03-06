import os

import pytest
from pykka import ActorRegistry

from chouette_iot.metrics.plugins._tegrastats_collector import (
    TegrastatsCollector,
    TegrastatsCollectorPlugin,
    WrappedMetric,
)
from chouette_iot.metrics.plugins.messages import StatsRequest, StatsResponse

TEGRASTATS_RESPONSE = (
    "RAM 6266/7860MB (lfb 1x1MB) CPU [20%@345,off,off,15%@345,15%@345,19%@345]"
    " EMC_FREQ 0% GR3D_FREQ 0% PLL@52C MCPU@52C PMIC@100C Tboard@50C GPU@51.5C"
    " BCPU@52C thermal@51.8C Tdiode@50C VDD_SYS_GPU 194/194"
    " VDD_SYS_SOC 389/389 VDD_4V0_WIFI 0/0 VDD_IN 2144/2144 VDD_SYS_CPU 194/194"
    " VDD_SYS_DDR 288/288\n"
)


@pytest.fixture(scope="module")
def tegrastats_mock():
    """
    Fixture for a fake Tegrastats utility.
    """
    path = "/tmp/tegrastats"
    with open(path, "w") as tegra_result:
        tegra_result.write(
            f"#!/bin/sh\n" f"echo '{TEGRASTATS_RESPONSE}'\n" f"echo 'Hello world!'"
        )
    os.chmod(path, 0o755)
    return path


@pytest.fixture
def tegraplugin_ref(tegrastats_mock, monkeypatch):
    """
    TegrastatsCollectorPlugin ActorRef fixture.
    """
    ActorRegistry.stop_all()
    monkeypatch.setenv("TEGRASTATS_PATH", tegrastats_mock)
    actor_ref = TegrastatsCollectorPlugin.get_instance()
    yield actor_ref
    ActorRegistry.stop_all()


def test_tegraplugin_handles_stats_request(tegraplugin_ref, test_actor):
    """
    TegrastatsCollectorPlugin sends back a valid StatsResponse message.

    GIVEN: There is a valid tegrastats utility.
    WHEN: TegrastatsCollectorPlugin receives a StatsRequest message.
    THEN: It sends back a StatsResponse message.
    AND: Its sender is TegrastatsCollectorPlugin.
    AND: Its stats property is an Iterator over WrappedMetric objects.
    """
    tegraplugin_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    assert isinstance(response, StatsResponse)
    assert response.producer == "TegrastatsCollectorPlugin"
    stats = response.stats
    assert all(isinstance(elem, WrappedMetric) for elem in stats)


def test_empty_list_on_no_tegrastats(monkeypatch, test_actor):
    """
    TegrastatsCollectorPlugin returns an empty Iterator on no Tegrastats utility.

    GIVEN: There is no valid tegrastats utility.
    WHEN: TegrastatsCollectorPlugin receives a StatsRequest message.
    THEN: It sends back a StatsResponse message.
    AND: Its stats property is an empty Iterator.
    """
    monkeypatch.setenv("TEGRASTATS_PATH", "/tmp/noactualfile")
    tegraplugin_ref = TegrastatsCollectorPlugin.get_instance()
    tegraplugin_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    assert isinstance(response, StatsResponse)
    assert response.producer == "TegrastatsCollectorPlugin"
    stats = response.stats
    assert not list(stats)


def test_tegraplugin_collect_stats(tegrastats_mock):
    """
    TegrastatsCollector's 'collect_stats' method returns an Iterator
    over WrappedMetrics.

    GIVEN: There is a valid Tegrastats utility.
    WHEN: TegrastatsCollector's 'collect_stats' method is called.
    THEN: It returns an Iterator over WrappedMetrics.
    """
    result = list(TegrastatsCollector.collect_stats(tegrastats_mock, ["ram", "temp"]))
    assert all(isinstance(metric, WrappedMetric) for metric in result)


def test_tegraplugin_get_raw_metrics(tegrastats_mock):
    """
    TegrastatsCollector's '_get_raw_metrics_string` method returns a
    single first line of Tegrastats output.

    GIVEN: There is a valid Tegrastats utility.
    AND: It's ready to write more than 1 line into stdout.
    WHEN: '_get_raw_metrics_string' method is called.
    THEN: A single first line of Tegrastats' output is returned.
    """
    raw_metrics = TegrastatsCollector._get_raw_metrics_string(tegrastats_mock)
    assert raw_metrics == TEGRASTATS_RESPONSE


def test_tegraplugin_get_temp_metrics():
    """
    TegrastatsCollector's '_get_temp_metrics' method gets temperature data
    from a raw metrics string.

    GIVEN: There is a valid Tegrastats response.
    WHEN: It's being passed to the '_get_temp_metrics' method.
    THEN: It returns a list of WrappedMetrics.
    AND: There is no metric with a tag ["zone:PMIC"].
    """
    temp_metrics = list(TegrastatsCollector._get_temp_metrics(TEGRASTATS_RESPONSE))
    assert all(isinstance(metric, WrappedMetric) for metric in temp_metrics)
    zones = [metric.tags for metric in temp_metrics]
    assert ["zone:PMIC"] not in zones


def test_tegraplugin_get_ram_metrics():
    """
    TegrastatsCollector's '_get_raw_metrics' method gets raw data
    from a raw metrics string.

    GIVEN: There is a valid Tegrastats response.
    WHEN: It's being passed to the '_get_ram_metrics' method.
    THEN: It returns 2 WrappedMetrics: ram.used and ram.free.
    """
    temp_metrics = list(TegrastatsCollector._get_ram_metrics(TEGRASTATS_RESPONSE))
    assert len(temp_metrics) == 2
    names = sorted([metric.metric for metric in temp_metrics])
    assert names == sorted(
        ["Chouette.tegrastats.ram.free", "Chouette.tegrastats.ram.used"]
    )


def test_tegraplugin_get_ram_metrics_wrong_raw_string():
    """
    TegrastatsCollector's '_get_raw_metrics' method returns an empty list on
    an incorrect raw metrics string.

    GIVEN: There is a raw Tegrastats response without any data about RAM.
    WHEN: It's being passed to the '_get_ram_metrics' method.
    THEN: It returns an empty iterator.
    """
    temp_metrics = list(
        TegrastatsCollector._get_ram_metrics("VRAM 6266/7860MB CPU [20%@345]")
    )
    assert temp_metrics == []


def test_tegraplugin_does_not_crash_on_stopped_sender(test_actor, tegraplugin_ref):
    """
    TegrastatsCollectorPlugin doesn't crash on stopped sender

    GIVEN: I have a working TegrastatsCollecltorPlugin actor.
    WHEN: Some actor sends a StatsRequest and stops before it gets a response.
    THEN: HostStatsCollector doesn't crash.
    """
    test_actor.stop()
    tegraplugin_ref.ask(StatsRequest(test_actor))
    assert tegraplugin_ref.is_alive()


def test_tegraplugin_does_not_crash_on_wrong_sender(tegraplugin_ref):
    """
    TegrastatsCollectorPlugin doesn't crash on wrong sender.

    GIVEN: I have a working TegrastatsCollectorPlugin actor.
    WHEN: Some actor sends a StatsRequest with some gibberish as a sender.
    THEN: HostStatsCollector doesn't crash.
    """
    tegraplugin_ref.ask(StatsRequest("not an actor"))
    assert tegraplugin_ref.is_alive()
