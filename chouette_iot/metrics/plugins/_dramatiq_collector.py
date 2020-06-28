"""
chouette.metrics.plugins.DramatiqCollector
"""
# pylint: disable=too-few-public-methods
import logging
import re
from itertools import chain
from typing import Iterator, List, Tuple

from redis import Redis, RedisError

from chouette_iot.configuration import RedisConfig
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
        config = RedisConfig()
        self.redis = Redis(host=config.redis_host, port=config.redis_port)

    def collect_stats(self) -> Iterator[WrappedMetric]:
        """
        Collects Dramatiq statistics from DramatiqCollector.

        Returns: Iterator over WrappedMetric objects.
        """
        hashes = self.get_hashes_names("dramatiq:*.msgs")
        sizes = self.get_hashes_sizes(hashes)
        return DramatiqCollector.wrap_queues_sizes(sizes)

    def get_hashes_names(self, pattern: str) -> List[bytes]:
        """
        Returns a list of hashes names satisfying a specified pattern.

        Args:
            pattern: Redis keys names pattern. E.g: 'dramatiq:*.msgs'.
        Returns: List of hashes names as bytes.
        """
        try:
            hashes_names = self.redis.keys(pattern)
        except RedisError as error:
            logger.warning(
                "[%s] Could not collect queues names for a pattern %s due to: '%s'.",
                self.name,
                pattern,
                error,
            )
            return []
        return hashes_names

    def get_hashes_sizes(self, hashes_names: List[bytes]) -> List[Tuple[str, int]]:
        """
        Returns a list of tuples with hashes names and sizes.

        Args:
            hashes_names: List of bytes with hashes names.
        Return: List of tuples with hashes names and sizes.
        """
        try:
            hash_sizes = [
                (
                    hash_name.decode() if isinstance(hash_name, bytes) else hash_name,
                    int(self.redis.hlen(hash_name)),
                )
                for hash_name in hashes_names
            ]
        except RedisError as error:
            logger.warning(
                "[%s] Could not calculate hash sizes due to: '%s'.", self.name, error
            )
            return []
        return hash_sizes


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
        if not sizes:
            return iter([])
        metrics = (
            cls._wrap_metrics(
                [("Chouette.dramatiq.queue_size", size)],
                tags={"queue": re.sub(r"^dramatiq:|.msgs$", "", queue)},
            )
            for queue, size in sizes
        )
        return chain.from_iterable(metrics)
