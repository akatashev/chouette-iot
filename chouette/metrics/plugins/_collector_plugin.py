"""
CollectorPlugin: Abstract class for all metric collectors.
"""
# pylint: disable=too-few-public-methods
from abc import ABC
from typing import Iterator, List, Optional, Tuple

from chouette.metrics import WrappedMetric

__all__ = ["CollectorPlugin"]


class CollectorPlugin(ABC):
    """
    Abstract class for all metric collectors.
    """

    @staticmethod
    def _wrap_metrics(
        metrics: List[Tuple[str, float]],
        timestamp: float = None,
        tags: Optional[List[str]] = None,
        metric_type: str = "gauge",
    ) -> Iterator:
        """
        Generates an iterator over WrappedMetric objects.

        Args:
            metrics: List of (metric_name, metric_value) tuples.
            timestamp: Metric collection timestamp.
            tags: Metric tags.
            metric_type: Metric type.
        Returns: Iterator over WrappedMetric objects.
        """
        return map(
            lambda metric: WrappedMetric(
                metric[0], metric_type, metric[1], timestamp, tags
            ),
            metrics,
        )
