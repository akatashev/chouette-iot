from chouette.metrics.plugins import PluginsFactory
from chouette.metrics.plugins import HostStatsCollector

from pykka import ActorRef


def test_plugins_factory_returns_actor_ref():
    """
    Plugins Factory returns an ActorRef of a plugin if it knows it.

    GIVEN: 'host' plugin name is associated with HostStatsCollector.
    WHEN: Someone requests a plugin 'host' via a .get_plugin method.
    THEN: HostStatsCollector ActorRef is returned.
    """
    response = PluginsFactory.get_plugin("host")
    assert isinstance(response, ActorRef)
    assert isinstance(response.actor_class, HostStatsCollector.__class__)


def test_plugins_factory_returns_none():
    """
    Plugins Factory returns None if it doesn't know what plugin to return.

    GIVEN: '~*{magic}*~' plugin name is not associated with any class.
    WHEN: Someone requests a plugin '~*{magic}*~' via a .get_plugin method.
    THEN: None is returned.
    """
    response = PluginsFactory.get_plugin('~*{magic}*~')
    assert response is None
