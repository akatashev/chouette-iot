import os

import pytest
import requests_mock
from pykka import ActorRegistry
from redis import Redis
from requests.exceptions import ConnectTimeout

from chouette import ChouetteConfig
from chouette._singleton_actor import SingletonActor


@pytest.fixture(scope="session")
def test_actor_class():
    """
    Test actor class fixture.
    """

    class TestActor(SingletonActor):
        """
        TestActor class.
        """

        def __init__(self):
            super().__init__()
            self.messages = []

        def on_receive(self, message):
            """
            On any message that is not a string "messages" or "count"
            it adds this message to a list of received messages.

            On "count" message it returns the size of this list.
            On "messages" message it returns this list itself.
            """
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
def metrics_keys():
    """
    Metrics keys fixture. Emulates how metrics keys are being returned
    from Redis: (metric key, addition timestamp).
    """
    return [
        (b"metric-uuid-1", 10),  # Keys group 1
        (b"metric-uuid-2", 12),  # Keys group 1
        (b"metric-uuid-3", 23),  # Keys group 2
        (b"metric-uuid-4", 31),  # Keys group 3
        (b"metric-uuid-5", 34),  # Keys group 3
    ]


@pytest.fixture(scope="session")
def raw_metrics_values():
    """
    Metrics values fixture. Emulates how metrics values are stored in Redis:
    (metric key, metric value).
    """
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
    redis_client.delete("chouette-iot:raw:metrics.keys")
    for key, ts in metrics_keys:
        redis_client.zadd("chouette-iot:raw:metrics.keys", {key: ts})
    yield metrics_keys
    redis_client.delete("chouette-iot:raw:metrics.keys")


@pytest.fixture
def stored_raw_values(redis_client, raw_metrics_values):
    """
    Fixture that stores dummy raw metrics values to Redis.

    Before and after every test queue hash is being cleaned up.
    """
    redis_client.delete("chouette-iot:raw:metrics.values")
    for key, message in raw_metrics_values:
        redis_client.hset("chouette-iot:raw:metrics.values", key, message)
    yield raw_metrics_values
    redis_client.delete("chouette-iot:raw:metrics.values")


@pytest.fixture
def redis_cleanup(redis_client):
    """
    Fixture that wraps a test with Redis cleanups.
    """
    redis_client.flushall()
    yield True
    redis_client.flushall()


@pytest.fixture(scope="session")
def k8s_stats_response():
    """
    Returns K8s /stats/summary endpoint output from microk8s on Jetson Nano.
    """
    with open("tests/resources/k8s_response.json", "r") as response_file:
        response = response_file.read()
    return response


@pytest.fixture(scope="session")
def docker_stats_response():
    """
    Returns docker socket stats for a Redis container:
    """
    with open("tests/resources/docker_stats.json", "r") as response_file:
        response = response_file.read()
    return response


@pytest.fixture
def mocked_http(monkeypatch, k8s_stats_response, docker_stats_response, requests_mock):
    """
    Datadog host, K8s stats and Docker stats mocking fixture.

    Datadog host:
    On API Key `correct` it returns 202 Accepted.
    On API Key `authfail` it returns 403 Authentication error.
    On API Key `exc` it raises a ConnectTimeout exception.

    K8s stats:
    /stats/summary returns a correct response.
    /stats/notjson returns 200 OK with a not JSON body.
    /stats/exc raises a ConnectTimeout exception.
    /stats/wrongcred returns 401 Unauthorized.

    Docker stats:
    /containers/json returns a correct json list.
    /not-json/json returns a not JSON body.
    /conn-exc/json raises a ConnectionError.
    /containers/123a/stats returns correct container stats.
    /containers/456b/stats returns a not JSON body.
    """
    monkeypatch.setenv("API_KEY", "correct")
    monkeypatch.setenv("GLOBAL_TAGS", '["host:pytest"]')
    monkeypatch.setenv("METRICS_BULK_SIZE", "3")
    monkeypatch.setenv("DATADOG_URL", "https://choeutte-iot.mock")
    datadog_url = ChouetteConfig().datadog_url
    # Datadog:
    requests_mock.register_uri("POST", "/v1/series?api_key=correct", status_code=202)
    requests_mock.register_uri("POST", "/v1/series?api_key=authfail", status_code=403)
    requests_mock.register_uri("POST", "/v1/series?api_key=exc", exc=ConnectTimeout)
    # K8S:
    requests_mock.register_uri("GET", "/stats/summary", text=k8s_stats_response)
    requests_mock.register_uri("GET", "/stats/notjson", text='{"node": []')
    requests_mock.register_uri("GET", "/stats/exc", exc=ConnectTimeout)
    requests_mock.register_uri(
        "GET", "/stats/wrongcreds", status_code=401, text="Unauthorized"
    )
    # Docker:
    requests_mock.register_uri("GET", "/containers/json", text='[{"Id": "123a"}]')
    requests_mock.register_uri("GET", "/not-json/json", text="Go away, no JSON here.")
    requests_mock.register_uri("GET", "/conn-exc/json", exc=ConnectionError)
    requests_mock.register_uri(
        "GET", "/containers/123a/stats?stream=false", text=docker_stats_response
    )
    requests_mock.register_uri(
        "GET", "/containers/456b/stats?stream=false", text="Go away, no JSON here."
    )
    yield datadog_url
