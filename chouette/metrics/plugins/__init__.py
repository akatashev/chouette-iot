"""
chouette.metrics.plugins
"""
# pylint: disable=too-few-public-methods
from typing import Optional

from pykka import ActorRef

from ._host_collector import HostStatsCollector

__all__ = ["HostStatsCollector", "PluginsFactory"]


class PluginsFactory:
    """
    PluginsFactory class creates plugins actors and returns their ActorRefs.
    """

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
        return plugin_class.get_instance()
