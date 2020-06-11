"""
MetricsCollector class.
"""
import logging
from typing import Any

from pykka import ActorRef, ActorRegistry  # type: ignore

from chouette_iot import ChouetteConfig
from chouette_iot._singleton_actor import VitalActor
from chouette_iot.metrics.plugins import PluginsFactory
from chouette_iot.metrics.plugins.messages import StatsRequest, StatsResponse
from chouette_iot.storages import RedisStorage
from chouette_iot.storages.messages import StoreRecords

logger = logging.getLogger("chouette-iot")

__all__ = ["MetricsCollector"]


class MetricsCollector(VitalActor):
    """
    Actor that is responsible for collecting various stats from a host
    and to store gathered data to a storage for later releasing.
    """

    def __init__(self):
        """
        On creation MetricsCollector reads a list of its plugins from
        environment variables.
        """
        super().__init__()
        config = ChouetteConfig()
        self.plugins = config.collector_plugins
        logger.info(
            "[%s] Starting. Configured collection plugins are: '%s'.",
            self.name,
            "', '".join(self.plugins),
        )

    def on_receive(self, message: Any) -> None:
        """
        On any message that is not a StatResponse one, MetricsCollector
        iterates over its plugins ActorRefs and sends them a StatsRequest
        message.

        They are expected to respond with a StatsResponse message.
        On this message MetricsCollector sends a request to a storage to store
        received metrics.

        Args:
            message: Can be anything.
        """
        if isinstance(message, StatsResponse):
            sender = message.producer
            logger.info("[%s] Storing collected stats from '%s'.", self.name, sender)
            redis = RedisStorage.get_instance()
            redis.tell(StoreRecords("metrics", message.stats, wrapped=True))
        else:
            plugins = map(PluginsFactory.get_plugin, self.plugins)
            for plugin in filter(None, plugins):  # type: ActorRef
                logger.info(
                    "[%s] Requesting stats from '%s'.",
                    self.name,
                    plugin.actor_class.__name__,
                )
                plugin.tell(StatsRequest(self.actor_ref))
