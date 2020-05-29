"""
MetricsCollector class.
"""
import logging
from typing import Any

from pykka import ActorRef

from chouette import ChouetteConfig
from chouette._singleton_actor import SingletonActor
from chouette.metrics.plugins import PluginsFactory
from chouette.metrics.plugins.messages import StatsRequest, StatsResponse
from chouette.storages import RedisStorage
from chouette.storages.messages import StoreRecords

logger = logging.getLogger("chouette")

__all__ = ["MetricsCollector"]


class MetricsCollector(SingletonActor):
    """
    Actor that is responsible for collecting various stats from a machine
    and to store gathered data to Redis for later releasing.
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
        On this message MetricsCollector sends a request to Redis to store
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
