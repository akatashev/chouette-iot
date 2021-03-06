"""
MetricsMerger object that is used in the MetricsAggregator workflow.
"""
import json
from functools import reduce
from itertools import groupby
from typing import List, Iterable, Tuple

from ._metrics import MergedMetric

__all__ = ["MetricsMerger"]


class MetricsMerger:
    """
    MetricsMerger class is a part of a MetricsAggregator workflow.

    It's responsible for separating raw metrics into groups and creating
    MergedMetric objects from raw data.
    Then these MergedMetric objects can be processed by a MetricsWrapper.
    """

    @staticmethod
    def group_metric_keys(
        keys_and_ts: List[Tuple[bytes, float]], flush_interval: int
    ) -> List[List[bytes]]:
        """
        Takes a list of raw keys information, received from a storage and
        produces a list of groups of keys of metrics emitted during the
        same `interval` seconds.

        Input example:
        [(b"key-1", 9), (b"key-2", 11), (b"key-3", 18), (b"key-4", 21)]
        With default interval that is 10 output will be:
        [[b"key-1"], [b"key-2", b"key-3"], [b"key-4"]]
        They represent groups of events, happened between:
        Seconds: [[0-9], [10-19], [20-29]]

        Args:
            keys_and_ts: List of tuples (key, timestamp).
            flush_interval: Length of a `flush interval` for aggregation.
        Returns: List of lists with grouped metrics keys.
        """
        grouped_keys_and_ts = groupby(
            keys_and_ts, lambda record: record[1] // flush_interval
        )
        keys = [[keys for keys, ts in group[1]] for group in grouped_keys_and_ts]
        return keys

    @classmethod
    def merge_metrics(cls, records: List[bytes], interval: int) -> List[MergedMetric]:
        """
        Takes a list of bytes presumably representing JSON encoded raw
        metrics and tries to cast them to a list of MergedMetrics.

        Args:
            records: List of bytes objects representing raw metrics as jsons.
            interval: Flush interval value.
        Returns: List of MergedMetric objects.
        """
        single_metrics = cls._cast_to_merged_metrics(records, interval)
        grouped_metrics = groupby(single_metrics, lambda metric: metric.id)
        merged_metrics = [
            reduce(lambda a, b: a + b, metrics) for _, metrics in grouped_metrics
        ]
        return merged_metrics

    @staticmethod
    def _cast_to_merged_metrics(
        records: Iterable[bytes], interval: int
    ) -> Iterable[MergedMetric]:
        """
        Generator Function that takes an iterable of bytes and tries to cast
        these object to MergesMetrics.

        Args:
            records: Bytes objects, presumably representing raw metrics.
            interval: Flush interval value.
        Return: Iterable of MergedMetric instances.
        """
        for record in records:
            try:
                dict_metric = json.loads(record)
                merged_metric = MergedMetric(
                    metric=dict_metric["metric"],
                    type=dict_metric["type"],
                    timestamps=[dict_metric["timestamp"]],
                    values=[dict_metric["value"]],
                    tags=dict_metric.get("tags"),
                    interval=interval,
                )
            except (json.JSONDecodeError, TypeError, KeyError):
                continue
            yield merged_metric
