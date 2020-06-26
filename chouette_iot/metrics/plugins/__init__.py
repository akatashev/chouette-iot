"""
chouette.metrics.plugins
"""
# pylint: disable=too-few-public-methods
from typing import Dict, Optional, Type

from pykka import ActorRef  # type: ignore

from ._collector_plugin import CollectorPluginActor
from ._docker_collector import DockerCollectorPlugin
from ._dramatiq_collector import DramatiqCollectorPlugin
from ._host_collector import HostCollectorPlugin
from ._k8s_collector import K8sCollectorPlugin
from ._tegrastats_collector import TegrastatsCollectorPlugin

__all__ = [
    "PluginsFactory",
]


class PluginsFactory:
    """
    PluginsFactory class creates plugins actors and returns their ActorRefs.
    """

    plugins: Dict[str, Type[CollectorPluginActor]] = {
        "dramatiq": DramatiqCollectorPlugin,
        "host": HostCollectorPlugin,
        "k8s": K8sCollectorPlugin,
        "tegrastats": TegrastatsCollectorPlugin,
        "docker": DockerCollectorPlugin,
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
