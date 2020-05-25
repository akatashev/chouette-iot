"""
Concrete implementation of a simple wrapper.
"""
from functools import reduce

from chouette.metrics import WrappedMetric
from ._metrics_wrapper import MetricsWrapper

__all__ = ["SimpleWrapper"]


class SimpleWrapper(MetricsWrapper):
    """
    SimpleWrapper class takes only two kinds of metrics:
    `count` and `gauge`.

    Unlike standard Datadog, it doesn't send the last value for the `gauge`
    metric. Instead it sends an average value along with a count of metrics
    used to calculate this average value.

    Received metric types:
    `count` - sends a single `count` metric with a sum of values.
    `gauge` - sends a `gauge` metric with an average value and a `.count`
              metric with a number of metrics used to calculate the avg.
    """

    @classmethod
    def _wrap_metric(cls, merged_metric):
        try:
            timestamp, value = cls._calculate_metric_points(merged_metric)
        except TypeError:
            return []

        metrics = [
            WrappedMetric(
                metric=merged_metric.metric,
                metric_type=merged_metric.type,
                timestamp=timestamp,
                value=value,
                tags=merged_metric.tags,
            )
        ]
        if merged_metric.type == "gauge":
            metrics.append(
                WrappedMetric(
                    metric=f"{merged_metric.metric}.count",
                    metric_type=merged_metric.type,
                    timestamp=timestamp,
                    value=len(merged_metric.values),
                    tags=merged_metric.tags,
                )
            )
        return metrics

    @staticmethod
    def _calculate_metric_points(merged_metric) -> tuple:
        values = merged_metric.values
        value = reduce(lambda x, y: x + y, values)
        if merged_metric.type != "count":
            value = value / len(values)
        return max(merged_metric.timestamps), value
