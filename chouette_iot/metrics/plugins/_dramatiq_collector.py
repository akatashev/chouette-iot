"""
chouette.metrics.plugins.DramatiqCollector
"""
# pylint: disable=too-few-public-methods
import logging
import re
from itertools import chain
from typing import Iterator, List, Tuple

from pykka import ActorDeadError  # type: ignore

from chouette_iot._singleton_actor import SingletonActor
from chouette_iot.metrics import WrappedMetric
from chouette_iot.storages import RedisStorage
from chouette_iot.storages._redis_messages import GetHashSizes, GetRedisQueues
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse

__all__ = ["DramatiqCollector"]

logger = logging.getLogger("chouette-iot")


class DramatiqCollector(SingletonActor):
    """
    Actor that collects Dramatiq queues sizes.

    NB: Collectors MUST interact with plugins via `tell` pattern.
        `ask` pattern will return None.
    """

    def __init__(self):
        super().__init__()
        self.redis = RedisStorage.get_instance()

    def on_receive(self, message: StatsRequest) -> None:
        """
        On StatsRequest message collects Dramatiq queues sizes and
        sends them back in a StatsResponse message.

        On any other message does nothing.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest):
            hashes = self.redis.ask(GetRedisQueues("dramatiq:*.msgs"))
            sizes = self.redis.ask(GetHashSizes(hashes))
            stats = DramatiqCollectorPlugin.wrap_queues_sizes(sizes)
            if hasattr(message.sender, "tell"):
                try:
                    message.sender.tell(StatsResponse(self.name, stats))
                except ActorDeadError:
                    logger.warning(
                        "[%s] Requester is stopped. Dropping message.", self.name
                    )


class DramatiqCollectorPlugin(CollectorPlugin):
    """
    DramatiqPlugin that wraps received hashes sizes into WrappedMetrics.
    """

    @classmethod
    def wrap_queues_sizes(cls, sizes: List[Tuple[str, int]]) -> Iterator[WrappedMetric]:
        """
        Wraps received hashes sizes into WrappedMetrics.

        Args:
            sizes: List of tuples ("hash_name", hash_size as int).
        Returns: Iterator over WrappedMetric objects.
        """
        metrics = [
            cls._wrap_metrics(
                [("Chouette.dramatiq.queue_size", size)],
                tags=[f"queue:{re.sub(r'^dramatiq:|.msgs$', '', queue)}"],
            )
            for queue, size in sizes
        ]
        return chain.from_iterable(metrics)
