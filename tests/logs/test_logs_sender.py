import json
import time
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from pykka import ActorRegistry
from redis import RedisError
from redis.client import Pipeline

from chouette_iot.logs import LogsSender
from chouette_iot.storages.messages import CollectKeys


@pytest.fixture
def sender_actor(monkeypatch, mocked_http):
    """
    LogsSender Actor fixture.
    """
    monkeypatch.setenv("host", "test_host")
    actor_ref = LogsSender.get_instance()
    yield actor_ref
    ActorRegistry.stop_all()


@pytest.fixture
def sender_proxy(sender_actor):
    """
    LogsSender Actor Proxy fixture for actor methods testing.
    """
    return sender_actor.proxy()


@pytest.fixture
def stored_logs(redis_client, metrics_keys):
    """
    Fixture that stores dummy logs keys to Redis.

    Before and after every test queue set is being cleaned up.
    """
    redis_client.delete("chouette:logs:wrapped.keys")
    for key, ts in metrics_keys:
        redis_client.zadd("chouette:logs:wrapped.keys", {key: ts})
    yield metrics_keys
    redis_client.delete("chouette:logs:wrapped.keys")


@pytest.fixture
def expected_logs(redis_client, sender_proxy, redis_cleanup):
    """
    Fixture that stores dummy logs values to Redis and
    returns expected logs for `collect_records` method tests.

    Before and after every test queue hash is being cleaned up.
    """
    logs = {
        "key-1": {
            "date": datetime(
                year=2012, month=12, day=21, tzinfo=timezone.utc
            ).isoformat(),
            "message": {"msg": "Hello, world!"},
            "level": "INFO",
            "ddsource": "test",
            "service": "test",
            "ddtags": [],
        },
    }
    # Adding a "log" that is not JSON parseable:
    redis_client.zadd("chouette:logs:wrapped.keys", {"wrong-log-uid": time.time()})
    redis_client.hset("chouette:logs:wrapped.values", b"wrong-log-uid", b"So wrong!")
    # Generate expected logs by adding global tags to tags fields.
    global_tags = sender_proxy.tags.get()
    for key, log in logs.items():
        redis_client.zadd("chouette:logs:wrapped.keys", {key: time.time()})
        redis_client.hset("chouette:logs:wrapped.values", key, json.dumps(log))
        tags = log.get("ddtags", []) + global_tags
        log["ddtags"] = ",".join(tags)
        log["host"] = "test_host"
    yield list(logs.values())


def test_sender_returns_true(sender_actor, expected_logs):
    """
    LogsSender returns True after successful dispatch and
    cleans up dispatched logs.

    GIVEN: There are records in the logs queue.
    AND: Everything is fine.
    WHEN: LogsSender receives a message.
    THEN: It returns True.
    AND: Dispatched logs are removed from the queue.
    """
    result = sender_actor.ask("dispatch")
    assert result is True
    sender_proxy = sender_actor.proxy()
    keys = sender_proxy.collect_keys("logs").get()
    assert not keys


def test_sender_returns_true_on_no_keys(sender_actor, redis_client, redis_cleanup):
    """
    LogsSender returns True if there is nothing to dispatch.

    GIVEN: There are no logs in the logs queue.
    WHEN: LogsSender receives a message.
    THEN: It returns True, because its work is finished successfully.
    """
    result = sender_actor.ask("dispatch")
    assert result is True


@pytest.mark.parametrize("api_key", ["authfail", "exc"])
def test_sender_returns_false_on_dispatch_problems(
        monkeypatch, expected_logs, api_key, mocked_http
):
    """
    LogsSender returns False on dispatch problems.

    GIVEN: There are logs in the logs queue.
    AND: For some reason request to Datadog doesn't return 200 OK.
    WHEN: LogsSender receives a message.
    THEN: It returns False, because logs were not dispatched.
    AND: Logs are not deleted from the queue.
    """
    monkeypatch.setenv("API_KEY", api_key)
    ActorRegistry.stop_all()
    sender_actor = LogsSender.get_instance()
    result = sender_actor.ask("dispatch")
    assert result is False
    sender_proxy = sender_actor.proxy()
    keys = sender_proxy.collect_keys("logs").get()
    values = sender_proxy.collect_records(keys, "logs").get()
    assert values == expected_logs


def test_sender_returns_false_on_redis_problems(
        monkeypatch, expected_logs, sender_actor
):
    """
    LogsSender returns False on Redis problems during logs cleanup.

    GIVEN: There are logs in the logs queue.
    AND: Datadog works fine and returns 200.
    AND: On deletion attempt Redis returns RedisError
    WHEN: LogsSender receives a message.
    THEN: It returns False, because logs were not deleted.
    """
    with patch.object(Pipeline, "execute", side_effect=RedisError):
        result = sender_actor.ask("dispatch")
    assert result is False
    sender_proxy = sender_actor.proxy()
    keys = sender_proxy.collect_keys("logs").get()
    values = sender_proxy.collect_records(keys, "logs").get()
    assert values == expected_logs


def test_sender_collect_logs_returns_list_of_logs(
        sender_proxy, expected_logs
):
    """
    LogsSender's `collect_records` methods returns a list of JSON
    strings containing stored logs whose tags are updated with global tags.

    GIVEN: There are records in the logs queue.
    AND: One of the records is not a valid log.
    WHEN: Method `collect_records` is called with their keys.
    THEN: It returns a list of dicts.
    AND: These dicts represent previously stored log records.
    AND: Every log has global tags added to its `tags` property.
    AND: Invalid record is ignored.
    """
    keys = sender_proxy.collect_keys("logs").get()
    assert len(keys) == 2
    dicts = sender_proxy.collect_records(keys, "logs").get()
    assert dicts == expected_logs
    assert len(dicts) == 1


def test_sender_dispatch_to_datadog(sender_proxy, expected_logs):
    """
    LogsSender `dispatch_to_datadog` returns True on 202 Accepted.

    GIVEN: It's possible to connect to Datadog and api_key is correct.
    WHEN: `dispatch_to_datadog` method is called for valid logs.
    THEN: True is returned when 202 Accepted response is received.
    """
    result = sender_proxy.dispatch_to_datadog(expected_logs).get()
    assert result is True


@pytest.mark.parametrize("api_key", ["authfail", "exc"])
def test_sender_dispatch_to_datadog_problem(monkeypatch, expected_logs, api_key):
    """
    LogsSender `dispatch_to_datadog` returns False on 403 Auth error or
    Requests exception.

    GIVEN: It's possible to connect to Datadog and api_key is incorrect.
    WHEN: `dispatch_to_datadog` method is called for valid logs.
    THEN: False is returned if 202 Accepted wasn't returned.
    """
    monkeypatch.setenv("API_KEY", api_key)
    ActorRegistry.stop_all()
    sender_proxy = LogsSender.get_instance().proxy()
    result = sender_proxy.dispatch_to_datadog(expected_logs).get()
    assert result is False


@pytest.mark.parametrize("send_self_metrics", [False, True])
def test_sender_sends_self_metrics(monkeypatch, expected_logs, send_self_metrics):
    """
    LogsSender stores dispatched logs number and size if
    `send_self_metrics` option is set to True.

    Scenario 1:
    GIVEN: Option `send_self_metrics` is set to True.
    WHEN: `dispatch_to_datadog` method is called and executed successfully.
    THEN: 2 `chouette.logs.dispatched` raw metrics are stored to Redis.

    Scenario 2:
    GIVEN: Option `send_self_metrics` is set to False.
    WHEN: `dispatch_to_datadog` method is called and executed successfully.
    THEN: No raw metrics are stored to Redis.
    """
    monkeypatch.setenv("SEND_SELF_METRICS", str(send_self_metrics))
    ActorRegistry.stop_all()
    sender_proxy = LogsSender.get_instance().proxy()
    sender_proxy.dispatch_to_datadog(expected_logs).get()
    redis = sender_proxy.redis.get()
    # Sleep due to async ChouetteClient nature:
    time.sleep(0.1)
    keys = redis.ask(CollectKeys("metrics", wrapped=False))
    assert (len(keys) == 2) is send_self_metrics
