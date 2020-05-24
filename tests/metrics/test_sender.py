from chouette.metrics import MetricsSender
import pytest


@pytest.fixture
def sender_proxy(monkeypatch):
    """
    MetricsSender Actor Proxy fixture for actor methods testing.
    """
    monkeypatch.setenv("API_KEY", "whatever")
    monkeypatch.setenv("COLLECTOR_PLUGINS", '["ice-cream", "berries"]')
    actor_ref = MetricsSender.get_instance()
    yield actor_ref.proxy()
    actor_ref.stop()

