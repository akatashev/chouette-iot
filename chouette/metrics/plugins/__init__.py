"""
chouette.metrics.plugins
"""
from typing import Optional

from pykka import ActorRef, ActorRegistry
from typing import Iterator

from ._host_collector import HostStatsCollector

__all__ = ["HostStatsCollector", "PluginsFactory"]


class PluginsFactory:
    """
    PluginsFactory class creates plugins actors and returns their ActorRefs.
    """

    # pylint: disable=too-few-public-methods
    @classmethod
    def get_plugin(cls, plugin_name: str) -> Optional[ActorRef]:
        """
        Takes a plugin name and returns an ActorRef if such plugin exists.

        Args:
            plugin_name: Plugin name as a string.
        Returns: ActorRef or None.
        """
        if plugin_name == "host":
            plugin_class = HostStatsCollector
        else:
            return None
        return cls._get_plugin(plugin_class)

    @staticmethod
    def _get_plugin(plugin_class):
        """
        Starts a plugin actor or gets a running instance from ActorRegistry.

        Args:
            plugin_class: Class of a plugin actor.
        Returns: ActorRef.
        """
        plugin_actors = ActorRegistry.get_by_class(plugin_class)
        if plugin_actors:
            return plugin_actors.pop()
        return plugin_class.start()
