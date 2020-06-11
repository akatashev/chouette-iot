"""
Concrete simplified implementation of a Datadog wrapper.
"""
# pylint: disable=too-few-public-methods
import math
from typing import Any, List, Set

from pydantic import BaseSettings  # type: ignore

from chouette_iot.metrics import MergedMetric, WrappedMetric
from ._metrics_wrapper import MetricsWrapper

__all__ = ["DatadogWrapper"]


class DatadogWrapperConfig(BaseSettings):
    """
    Optional Wrapper configuration object.
    """

    histogram_aggregates: List[str] = ["max", "median", "avg", "count"]
    histogram_percentiles: List[float] = [0.95]


class DatadogWrapper(MetricsWrapper):
    """
    Datadog wrapper tries to implement the same behaviour as Datadog agent
    that is described here:
    https://docs.datadoghq.com/developers/metrics/types/

    That is not some really accurate implementation of this behaviour, that's
    an attempt to have a Datadog-like aggregation strategy.

    Supported metrics:
    COUNT - sends a sum of values received during a flush interval.
    GAUGE - sends the last value received during a flush interval.
    RATE - sends a number of events happened during 1 seconds of a
           flush interval.
    SET - sends a count of unique elements send in a metric during a
           flush interval.
    HISTOGRAM - sends a set of different metrics according to
           histogram_aggregates and histogram_percentiles configuration.

    DISTRIBUTION metric type is NOT supported.
    """

    histogram_percentiles = DatadogWrapperConfig().histogram_percentiles
    histogram_aggregates = DatadogWrapperConfig().histogram_aggregates

    @classmethod
    def _wrap_metric(cls, merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Facade wrapper function of the Datadog Wrapper.

        It processes: COUNT, GAUGE, RATE, SET and HISTOGRAM metrics.

        All other kinds of metrics are ignored.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        methods = {
            "count": cls._wrap_count,
            "rate": cls._wrap_rate,
            "gauge": cls._wrap_gauge,
            "set": cls._wrap_set,
            "histogram": cls._wrap_histogram,
        }
        method = methods.get(merged_metric.type)
        if not method:
            return []
        return method(merged_metric)

    @staticmethod
    def _wrap_count(merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Count metric is being simply wrapped into a metric whose value is a
        sum of all the values and whose timestamp is the earliest timestamp
        in a sequence.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        count_metric = WrappedMetric(
            metric=merged_metric.metric,
            type=merged_metric.type,
            timestamp=min(merged_metric.timestamps),
            value=sum(merged_metric.values),
            tags=merged_metric.s_tags,
            interval=merged_metric.interval,
        )
        return [count_metric]

    @classmethod
    def _wrap_rate(cls, merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Rate metric represents an approximate rate of event occurrences per
        one second of the flush interval.
        Like count it calculates the sum of all the values but then this
        value is divided by a number of seconds in the flush interval.
        Timestamp is the earliest timestamp in a sequence.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        flush_interval = float(merged_metric.interval)
        rate_metric = WrappedMetric(
            metric=merged_metric.metric,
            type=merged_metric.type,
            timestamp=min(merged_metric.timestamps),
            value=sum(merged_metric.values) / flush_interval,
            tags=merged_metric.s_tags,
            interval=merged_metric.interval,
        )
        return [rate_metric]

    @staticmethod
    def _wrap_gauge(merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Gauge metric sends the latest received value without additional
        calculations. Its timestamp however is the earliest timestamp
        in a sequence.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        data_pairs = zip(merged_metric.values, merged_metric.timestamps)
        value, _ = max(data_pairs, key=lambda pair: pair[1])
        gauge_metric = WrappedMetric(
            metric=merged_metric.metric,
            type=merged_metric.type,
            timestamp=min(merged_metric.timestamps),
            value=value,
            tags=merged_metric.s_tags,
        )
        return [gauge_metric]

    @staticmethod
    def _wrap_set(merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Set metric is expected to have `Lists` in values. There lists
        should represent sets of data. Wrapper takes these lists and
        collects how many unique elements they contained.

        E.g.:
        On second 1 metric 'users' was produced with the following list:
        [Alice, Bob].
        On second 9 metric 'users' was produces with the following list:
        [Bob, Carol].

        This wrapper takes both lists, merge them into a single list:
        [Alice, Bob, Bob, Carol]
        and then calculates the number of unique elements.
        In this case it's 3.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        try:
            values_set: Set[Any] = set(sum(merged_metric.values, []))
            set_count_metric = WrappedMetric(
                metric=merged_metric.metric,
                type="count",
                timestamp=min(merged_metric.timestamps),
                value=len(values_set),
                tags=merged_metric.s_tags,
                interval=merged_metric.interval,
            )
            return [set_count_metric]
        except TypeError:
            return []

    @classmethod
    def _wrap_histogram(cls, merged_metric: MergedMetric) -> List[WrappedMetric]:
        """
        Histogram metric wrapper implementation.

        It depends on HISTOGRAM_PERCENTILES and HISTOGRAM_AGGREGATES
        configuration.

        These configuration options are described here:
        https://docs.datadoghq.com/developers/metrics/types/#?tab=histogram

        By default it generates exactly the same 5 metrics:
        `avg`, `count`, `max`, `median`, `95percentile`
        Other metrics like `sum`, `min` or other percentiles can be
        configured via configuration mentioned above.

        To avoid bringing Numpy just to calculate percentiles, a `_percentile`
        method is used. In tests it provided the same values as numpy.

        Args:
            merged_metric: MergedMetric to wrap.
        Returns: List of WrappedMetric produced by the wrapping method.
        """
        interval = float(merged_metric.interval)
        timestamp = min(merged_metric.timestamps)
        tags = merged_metric.tags
        values = merged_metric.values
        name = merged_metric.metric
        metrics_count = len(values)
        metrics_to_generate = [
            (f"{name}.avg", "gauge", sum(values) / metrics_count, None),
            (f"{name}.count", "rate", metrics_count / interval, int(interval)),
            (f"{name}.sum", "gauge", sum(values), None),
            (f"{name}.min", "gauge", min(values), None),
            (f"{name}.max", "gauge", max(values), None),
            (f"{name}.median", "gauge", cls._percentile(values, 0.5), None),
        ]
        percentiles_metrics_to_generate = [
            (
                f"{name}.{int(percentile * 100)}percentile",
                "gauge",
                cls._percentile(values, percentile),
                None,
            )
            for percentile in cls.histogram_percentiles
        ]
        metrics_to_generate.extend(percentiles_metrics_to_generate)
        generated_metrics = [
            WrappedMetric(
                metric=metric_name,
                type=metric_type,
                timestamp=timestamp,
                value=value,
                tags=tags,
                interval=interval,
            )
            for metric_name, metric_type, value, interval in metrics_to_generate
            if "percentile" in metric_name
            or metric_name.split(".")[-1] in cls.histogram_aggregates
        ]
        return generated_metrics

    @staticmethod
    def _percentile(data_set: List[float], percent: float) -> float:
        """
        Since we just need to calculate percentile and median and we don't
        want to bring the whole numpy here for this task.

        Args:
            data_set: List of float or integer values.
            percent: What percentile should be returned.
        return: Float value of a percentile.
        """
        sorted_ds = sorted(data_set)
        idx = (len(sorted_ds) - 1) * percent
        if idx % 1 == 0:
            return sorted_ds[int(idx)]
        upper_idx = math.ceil(idx)
        lower_idx = math.floor(idx)
        right_value = sorted_ds[upper_idx] * (idx - lower_idx)
        left_value = sorted_ds[lower_idx] * (upper_idx - idx)
        return left_value + right_value
