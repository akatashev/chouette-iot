"""
Concrete implementation of a simple wrapper.
"""
# pylint: disable=too-few-public-methods
from typing import List

from ._metrics_wrapper import MetricsWrapper

__all__ = ["SimpleWrapper"]

from .._metrics import WrappedMetric, MergedMetric


class SimpleWrapper(MetricsWrapper):
    """
    SimpleWrapper class takes only two kinds of metrics:
    `count` and `gauge`. Any metrics that is not `count` is being cast
    into a `gauge` metrics that, in fact, is not a `gauge` in Datadog sense.

    Unlike standard Datadog, it doesn't send the last value for the `gauge`
    metric. Instead it sends an average value along with a count of metrics
    used to calculate this average value.

    Received metric types:
    `count`:
            sends a single `count` metric with a sum of values.
     anything else:
            sends a `gauge` metric with an average value and a `.count`
            metric with a number of metrics used to calculate the avg.
    """

    @classmethod
    def _wrap_metric(cls, merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        This method determines what method to call to wrap this exact metric
        and calls it.

        For this wrapper it knows only two methods - a method to wrap `count`
        metrics and a method to wrap anything else.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        methods = {"count": cls._wrap_count}
        metric_type = merged_metric.type
        return methods.get(metric_type, cls._wrap_average)(merged_metric)

    @classmethod
    def _wrap_count(cls, merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Count metric is being simply wrapped into a metric whose value is a
        sum of all the values and whose timestamp is the latest timestamp
        in a sequence.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        count_metric = cls._create_wrapped_metric(
            metric_name=merged_metric.metric,
            metric_type=merged_metric.type,
            timestamp=max(merged_metric.timestamps),
            value=sum(merged_metric.values),
            tags=merged_metric.tags,
            interval=merged_metric.interval,
        )
        return [count_metric]

    @classmethod
    def _wrap_average(cls, merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Average metric is represented by two WrappedMetrics:
        1. `gauge` type metric whose value is the average of the original
        `values` list.
        2. `count` metric whose value is the number of elements in the
        `values` list used for average calculation. Its name is basically
        the name of the `gauge` metrics with '.count' suffix.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        metrics_count = len(merged_metric.values)
        average = sum(merged_metric.values) / metrics_count
        name = merged_metric.metric
        timestamp = max(merged_metric.timestamps)
        tags = merged_metric.tags
        interval = merged_metric.interval
        to_generate = (
            (name, "gauge", timestamp, average, tags, None),
            (f"{name}.count", "count", timestamp, metrics_count, tags, interval),
        )
        return [cls._create_wrapped_metric(*metric) for metric in to_generate]
