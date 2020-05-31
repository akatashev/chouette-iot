"""
chouette.metrics.plugins
"""
# pylint: disable=too-few-public-methods
from typing import Dict, Optional, Type

from pykka import ActorRef

from chouette._singleton_actor import SingletonActor
from ._dramatiq_collector import DramatiqCollector
from ._host_collector import HostStatsCollector
from ._tegrastats_collector import TegrastatsCollector

__all__ = [
    "DramatiqCollector",
    "HostStatsCollector",
    "PluginsFactory",
    "TegrastatsCollector",
]


class PluginsFactory:
    """
    PluginsFactory class creates plugins actors and returns their ActorRefs.
    """

    plugins: Dict[str, Type[SingletonActor]] = {
        "dramatiq": DramatiqCollector,
        "host": HostStatsCollector,
        "tegrastats": TegrastatsCollector,
    }

    @classmethod
    def get_plugin(cls, plugin_name: str) -> Optional[ActorRef]:
        """
        Takes a plugin name and returns an ActorRef if such plugin exists.

        Args:
            plugin_name: Plugin name as a string.
        Returns: ActorRef or None.
        """
        plugin_class = cls.plugins.get(plugin_name)
        if not plugin_class:
            return None
        actor_ref: ActorRef = plugin_class.get_instance()
        return actor_ref
