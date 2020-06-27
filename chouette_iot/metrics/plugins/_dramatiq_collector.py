"""
chouette.metrics.plugins.DramatiqCollector
"""
# pylint: disable=too-few-public-methods
import logging
import re
from itertools import chain
from typing import Iterator, List, Tuple

from chouette_iot.storages import StoragesFactory
from chouette_iot.storages._redis_messages import GetHashSizes, GetRedisQueues
from ._collector_plugin import CollectorPluginActor, StatsCollector
from .._metrics import WrappedMetric

__all__ = ["DramatiqCollectorPlugin"]

logger = logging.getLogger("chouette-iot")


class DramatiqCollectorPlugin(CollectorPluginActor):
    """
    Actor that collects Dramatiq queues sizes.

    NB: Collectors MUST interact with plugins via `tell` pattern.
        `ask` pattern will return None.
    """

    def __init__(self):
        """
        This Collector for now works ONLY for Dramatiq that uses Redis
        as a broker, so `self.redis` is used here intentionally.
        """
        super().__init__()
        self.redis = StoragesFactory.get_storage("redis")

    def collect_stats(self) -> Iterator[WrappedMetric]:
        """
        Collects Dramatiq statistics from DramatiqCollector.

        Returns: Iterator over WrappedMetric objects.
        """
        hashes = self.redis.ask(GetRedisQueues("dramatiq:*.msgs"))
        sizes = self.redis.ask(GetHashSizes(hashes))
        return DramatiqCollector.wrap_queues_sizes(sizes)


class DramatiqCollector(StatsCollector):
    """
    StatsCollector that wraps received hashes sizes into WrappedMetrics.
    """

    @classmethod
    def wrap_queues_sizes(cls, sizes: List[Tuple[str, int]]) -> Iterator[WrappedMetric]:
        """
        Wraps received hashes sizes into WrappedMetrics.

        Args:
            sizes: List of tuples ("hash_name", hash_size as int).
        Returns: Iterator over WrappedMetric objects.
        """
        metrics = (
            cls._wrap_metrics(
                [("Chouette.dramatiq.queue_size", size)],
                tags={"queue": re.sub(r"^dramatiq:|.msgs$", "", queue)},
            )
            for queue, size in sizes
        )
        return chain.from_iterable(metrics)
