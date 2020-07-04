"""
chouette.metrics.plugins.DramatiqCollector
"""
# pylint: disable=too-few-public-methods
import logging
import re
from itertools import chain
from typing import Iterable, Iterator, List, Tuple

from pydantic import BaseSettings
from redis import Redis, RedisError

from ._collector_plugin import CollectorPluginActor, StatsCollector
from .._metrics import WrappedMetric

__all__ = ["DramatiqCollectorPlugin"]

logger = logging.getLogger("chouette-iot")


class DramatiqConfig(BaseSettings):
    """
    RedisStorage environment configuration object.
    Reads Redis' host and port from environment variables if called.
    """

    redis_host: str = "redis"
    redis_port: int = 6379


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

    def collect_stats(self) -> Iterator[WrappedMetric]:
        """
        Collects Dramatiq statistics from DramatiqCollector.

        Returns: Iterator over WrappedMetric objects.
        """
        return DramatiqCollector.collect_stats("dramatiq:*.msgs")


class DramatiqCollector(StatsCollector):
    """
    StatsCollector that wraps received hashes sizes into WrappedMetrics.
    """

    config = DramatiqConfig()
    redis = Redis(host=config.redis_host, port=config.redis_port)
    name = "DramatiqCollector"

    @classmethod
    def collect_stats(cls, pattern: str) -> Iterator[WrappedMetric]:
        queues_names = cls._get_queues_names(pattern)
        queues_sizes = cls._get_queues_sizes(queues_names)
        return cls._wrap_queues_sizes(queues_sizes)

    @classmethod
    def _get_queues_names(cls, pattern: str) -> List[bytes]:
        """
        Returns a list of hashes names satisfying a specified pattern.

        Args:
            pattern: Redis keys names pattern. E.g: 'dramatiq:*.msgs'.
        Returns: List of hashes names as bytes.
        """
        try:
            hashes_names = cls.redis.keys(pattern)
        except RedisError as error:
            logger.warning(
                "[%s] Could not collect queues names for a pattern %s due to: '%s'.",
                cls.name,
                pattern,
                error,
            )
            return []
        return hashes_names

    @classmethod
    def _get_queues_sizes(cls, hashes_names: List[bytes]) -> Iterable[Tuple[str, int]]:
        """
        Returns an iterator over tuples with hashes names and sizes.

        Args:
            hashes_names: List of bytes with hashes names.
        Return: Iterator over tuples with hashes names and sizes.
        """
        try:
            hash_sizes = [
                (
                    hash_name.decode() if isinstance(hash_name, bytes) else hash_name,
                    int(cls.redis.hlen(hash_name)),
                )
                for hash_name in hashes_names
            ]
        except RedisError as error:
            logger.warning(
                "[%s] Could not calculate hash sizes due to: '%s'.", cls.name, error
            )
            return []
        return hash_sizes

    @classmethod
    def _wrap_queues_sizes(
        cls, sizes: Iterable[Tuple[str, int]]
    ) -> Iterator[WrappedMetric]:
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
