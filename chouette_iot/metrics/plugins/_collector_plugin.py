"""
CollectorPlugin: Abstract classes for all metric collectors.
"""
import logging

# pylint: disable=too-few-public-methods
from abc import ABC
from typing import Generator, Iterator, List, Optional, Tuple

from pykka import ActorDeadError  # type: ignore

from chouette_iot._singleton_actor import SingletonActor
from .messages import StatsRequest, StatsResponse
from .._metrics import WrappedMetric

__all__ = ["CollectorPluginActor", "StatsCollector"]

logger = logging.getLogger("chouette-iot")


class CollectorPluginActor(SingletonActor):
    """
    Base abstract class for all Collector Plugin Actors.
    """

    def on_receive(self, message: StatsRequest) -> None:
        """
        Template Method fot Collector Plugin Actors message handling.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest):
            stats = self.collect_stats()
            if hasattr(message.sender, "tell"):
                try:
                    message.sender.tell(StatsResponse(self.name, stats))
                except ActorDeadError:
                    logger.warning(
                        "[%s] Requester is stopped. Dropping message.", self.name
                    )

    def collect_stats(self) -> Iterator[WrappedMetric]:
        """
        Abstract method for stats collection.

        Returns: Iterator over WrappedMetric objects.
        """
        raise NotImplementedError(
            "Use concrete CollectorPluginActor class."
        )  # pragma: no cover


class StatsCollector(ABC):
    """
    Abstract class for all stats metric collectors.
    """

    @staticmethod
    def _wrap_metrics(
        metrics: List[Tuple[str, float]],
        timestamp: float = None,
        tags: Optional[List[str]] = None,
        metric_type: str = "gauge",
    ) -> Generator[WrappedMetric, None, None]:
        """
        Generates a list of WrappedMetric objects.

        Args:
            metrics: List of (metric_name, metric_value) tuples.
            timestamp: Metric collection timestamp.
            tags: Metric tags.
            metric_type: Metric type.
        Returns: Generator of WrappedMetric objects.
        """
        wrapped_metrics: Generator[WrappedMetric, None, None] = (
            WrappedMetric(
                metric=metric_name,
                type=metric_type,
                value=metric_value,
                timestamp=timestamp,
                tags=tags,
            )
            for metric_name, metric_value in metrics
            if metric_value
        )
        return wrapped_metrics
