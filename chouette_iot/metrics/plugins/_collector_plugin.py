"""
CollectorPlugin: Abstract class for all metric collectors.
"""
# pylint: disable=too-few-public-methods
from abc import ABC
from typing import Iterator, List, Optional, Tuple

from chouette_iot.metrics import WrappedMetric

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
    ) -> Iterator[WrappedMetric]:
        """
        Generates a list of WrappedMetric objects.

        Args:
            metrics: List of (metric_name, metric_value) tuples.
            timestamp: Metric collection timestamp.
            tags: Metric tags.
            metric_type: Metric type.
        Returns: Iterator over WrappedMetric objects.
        """
        wrapped_metrics: List[WrappedMetric] = [
            WrappedMetric(
                metric=metric_name,
                type=metric_type,
                value=metric_value,
                timestamp=timestamp,
                tags=tags,
            )
            for metric_name, metric_value in metrics
            if metric_value
        ]
        return iter(wrapped_metrics)
