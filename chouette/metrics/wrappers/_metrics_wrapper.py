"""
MetricsWrapper abstract class.
"""
# pylint: disable=too-few-public-methods
from abc import ABC, abstractmethod
from typing import List

from chouette.metrics import MergedMetric, WrappedMetric

__all__ = ["MetricsWrapper"]


class MetricsWrapper(ABC):
    """
    MetricsWrapper is the very heart of the Aggregation system.

    Its purpose is to take "raw" metrics sent by other applications and to
    cast them to standard Datadog metrics in a way that you prefer.

    E.g.: When we use a @timer decorator in an application, in reality
    5 metrics are being sent instead of 1 or 2.
    What if you don't want to pay for these additional metrics, because
    you don't need them at all?

    You're able to create a custom MetricWrapper that will cast your "timer"
    metric to the set of metrics that you prefer. To just one average or
    just one max or just a median or whatever.
    """

    @classmethod
    def wrap_metrics(cls, merged_metrics: List[MergedMetric]) -> List[WrappedMetric]:
        """
        This is the only public method of a MetricsWrapper.

        It should take merged "raw" metrics and provide a list of wrapped
        metrics ready to be sent to Datadog.

        Args:
            merged_metrics: List of MergedMetric objects with raw metrics.
            flush_interval: Aggregation frequency in seconds.
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
