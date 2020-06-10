import pytest

from chouette.metrics import WrappedMetric
from chouette.metrics.plugins import DockerCollector
from chouette.metrics.plugins._docker_collector import DockerCollectorPlugin
from chouette.metrics.plugins.messages import StatsRequest, StatsResponse


@pytest.fixture
def docker_ref():
    """
    DockerCollector actor's ActorRef fixture.
    """
    ref = DockerCollector.get_instance()
    yield ref
    ref.stop()


def test_docker_plugin_handles_stats_request(mocked_http, docker_ref, test_actor):
    """
    DockerCollector is able to handle StatsResponse requests:

    GIVEN: K8s stats server is up and running.
    AND: Our cert and key are correct.
    WHEN: DockerCollector receives an StatsRequest message.
    THEN: It sends back a StatsResponse message.
    AND: This response contains WrappedMetric objects.
    """
    docker_ref.ask(StatsRequest(test_actor))
    response = test_actor.ask("messages").pop()
    assert isinstance(response, StatsResponse)
    assert response.producer == "DockerCollector"
    stats = response.stats
    assert stats
    assert all(isinstance(elem, WrappedMetric) for elem in stats)


def test_docker_plugin_does_not_crash_on_stopped_sender(test_actor, docker_ref):
    """
    DockerCollector doesn't crash on stopped sender

    GIVEN: I have a working DockerCollector actor.
    WHEN: Some actor sends a StatsRequest and stops before it gets a response.
    THEN: HostStatsCollector doesn't crash.
    """
    test_actor.stop()
    docker_ref.ask(StatsRequest(test_actor))
    assert docker_ref.is_alive()


def test_docker_plugin_does_not_crash_on_wrong_sender(docker_ref):
    """
    DockerCollector doesn't crash on wrong sender.

    GIVEN: I have a working DockerCollector actor.
    WHEN: Some actor sends a StatsRequest with some gibberish as a sender.
    THEN: HostStatsCollector doesn't crash.
    """
    docker_ref.ask(StatsRequest("not an actor"))
    assert docker_ref.is_alive()


def test_docker_plugin_gets_stats(mocked_http):
    """
    DockerCollectorPlugin returns stats on `collect_metrics` request.

    GIVEN: Docker is running and its socket is reachable.
    ANS: 1 container is running.
    WHEN: `collect_metrics` method is called.
    THEN: It returns a list of 2 metrics for this container.
    """
    result = DockerCollectorPlugin.collect_metrics(
        "http+unix://%2Fvar%2Frun%2Fdocker.sock/containers"
    )
    result_list = list(result)
    assert len(result_list) == 2


def test_docker_plugin_gets_containers_ids(mocked_http):
    """
    DockerCollectorPlugin gets containers ID.

    GIVEN: Docker is running and its socket is reachable.
    AND: 1 container is running.
    WHEN: `_get_containers_ids` method is called.
    THEN: It returns a list with 1 id.
    """
    result = DockerCollectorPlugin._get_containers_ids(
        "http+unix://%2Fvar%2Frun%2Fdocker.sock/containers"
    )
    assert result == ["123a"]


@pytest.mark.parametrize("endpoint", ["not-json", "conn-exc"])
def test_docker_plugin_gets_containers_ids_empty(endpoint, mocked_http):
    """
    DockerCollectorPlugin returns an empty list of ids on a problem.

    GIVEN: Docker is not running.
    OR: It's running but doesn't return a valid json on 'containers/json'.
    WHEN: `_get_containers_ids` method is called.
    THEN: It returns an empty list.
    """
    result = DockerCollectorPlugin._get_containers_ids(
        f"http+unix://%2Fvar%2Frun%2Fdocker.sock/{endpoint}"
    )
    assert not result


def test_docker_plugin_gets_container_stats(mocked_http):
    """
    DockerCollectorPlugin returns a list of container stats.

    GIVEN: Docker is running and its socket is reachable.
    AND: Containers are running.
    WHEN: `_get_container_stats` method is called for one of them.
    THEN: It returns an iterator for a list with 2 elements.
    AND: Both elements are WrappedMetrics.
    AND: One of the is a CPU metric and another is a memory metric.
    AND: Their types are 'gauge'.
    AND: Their tags are ['container:<container_name'].
    AND: Their values represent data returned by Docker.
    """
    result = DockerCollectorPlugin._get_container_stats(
        "123a", "http+unix://%2Fvar%2Frun%2Fdocker.sock/containers"
    )
    result_list = list(result)
    assert len(result_list) == 2
    metric1, metric2 = result_list
    # Memory usage check:
    assert metric1.metric == "Chouette.docker.memory.usage"
    assert metric1.tags == ["container:chouette-iot-iot_redis_1"]
    assert metric1.value == 11001856
    assert metric1.type == "gauge"
    # CPU usage check:
    assert metric2.metric == "Chouette.docker.cpu.usage"
    assert metric2.tags == ["container:chouette-iot-iot_redis_1"]
    assert metric2.value == 127550000000
    assert metric2.type == "gauge"


def test_docker_plugin_gets_container_stats_empty(mocked_http):
    """
    DockerCollectorPlugin returns an empty list of stats on a problem.

    GIVEN: Docker is not running.
    OR: It's running but doesn't return a valid json on container/id/stats.
    WHEN: `_get_container_stats` method is called.
    THEN: It returns an iterator of an empty list.
    """
    result = DockerCollectorPlugin._get_container_stats(
        "456b", "http+unix://%2Fvar%2Frun%2Fdocker.sock/containers"
    )
    assert not list(result)
