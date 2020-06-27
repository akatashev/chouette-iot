"""
MetricsWrapper abstract class.
"""
# pylint: disable=too-few-public-methods
from abc import ABC, abstractmethod
from typing import List

from .._metrics import MergedMetric, WrappedMetric

__all__ = ["MetricsWrapper"]


class MetricsWrapper(ABC):
    """
    MetricsWrapper is the very heart of the Aggregation system.

    Its purpose is to take "raw" metrics sent by other applications and to
    cast them to standard Datadog metrics in a way that you prefer.
    """

    @classmethod
    def wrap_metrics(cls, merged_metrics: List[MergedMetric]) -> List[WrappedMetric]:
        """
        This is the only public method of a MetricsWrapper.

        It should take merged "raw" metrics and provide a list of wrapped
        metrics ready to be sent to Datadog.

        Args:
            merged_metrics: List of MergedMetric objects with raw metrics.
        Returns: List of WrappedMetric objects ready to be sent to Datadog.
        """
        metrics = [cls._wrap_metric(metric) for metric in merged_metrics]
        return sum(metrics, [])

    @classmethod
    @abstractmethod
    def _wrap_metric(cls, merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Method that takes a single Merged Metric and casts it to a list
        of Wrapped Metrics.

        Args:
            merged_metric: A single MergedMetric to process.
        Returns: List of produced WrappedMetric objects.
        """
        raise NotImplementedError(
            "Use concrete Wrapper implementation."
        )  # pragma: no cover

    @classmethod
    def _create_wrapped_metric(
        cls,
        metric_name: str,
        metric_type: str,
        timestamp: float,
        value: float,
        tags: List[str],
        interval: int = None,
    ) -> WrappedMetric:
        """
        Generates a WrappedMetric object based on provided data.

        Args:
            metric_name: Name of a metric to be sent.
            metric_type: Type of a metric: 'gauge', 'count', 'rate', 'set' or 'histogram'.
            timestamp: Metric timestamp.
            value: Metric value as a float.
            tags: Metric tags as a list of strings.
            interval: Flash interval value used for some metrics.
        Returns: A WrappedMetric object.
        """
        wrapped_metric = WrappedMetric(
            metric=metric_name,
            type=metric_type,
            timestamp=timestamp,
            value=value,
            tags=tags,
            interval=interval,
        )
        return wrapped_metric
