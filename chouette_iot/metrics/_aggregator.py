import json
import logging
from functools import reduce
from itertools import groupby
from typing import Any, List, Optional, Tuple

from chouette_iot import ChouetteConfig
from chouette_iot._singleton_actor import VitalActor
from chouette_iot.metrics import MergedMetric
from chouette_iot.metrics.wrappers import WrappersFactory
from chouette_iot.storages import RedisStorage
from chouette_iot.storages.messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreRecords,
)

__all__ = ["MetricsAggregator"]

logger = logging.getLogger("chouette-iot")


class MetricsAggregator(VitalActor):
    """
    MetricsAggregator is an actor that is responsible for data aggregation.
    https://docs.datadoghq.com/developers/dogstatsd/data_aggregation/

    Environment variable `AGGREGATE_INTERVAL` determines how often data is
    aggregated and it's analogous to a `flush interval` from DogStatsD
    documentation. Collected metrics are being processed in groups,
    where every group represents all the metrics emitted during
    `AGGREGATE_INTERVAL` seconds.

    If Chouette was working constantly, it usually don't have lots of
    raw metrics to process, it's just some amount of metrics collected
    since the last Aggregation execution.
    However, if for some reason Chouette was stopped or down and other
    applications were producing metrics, grouping thousands of keys
    into small groups, gathering lots of groups of metrics and
    processing every group can take quite a while.

    To avoid duplicating metrics in this situation, MetricsAggregator
    intentionally has a blocking workflow based on `ask` patterns.

    If one aggregate call takes more than `AGGREGATE_INTERVAL` to
    finish, other calls will be queued in the actor's mailbox and
    will be executed only when the first call is finished and processed
    metrics are cleaned up from a storage.
    """

    def __init__(self):
        super().__init__()
        config = ChouetteConfig()
        self.aggregate_interval = config.aggregate_interval
        self.metric_ttl = config.metric_ttl
        self.metrics_wrapper = WrappersFactory.get_wrapper(config.metrics_wrapper)
        if not self.metrics_wrapper:
            logger.warning(
                "[%s] No MetricsWrapper found. "
                "Raw metrics won't be collected and aggregated.",
                self.name,
            )
        self.redis = None

    def on_receive(self, message: Any) -> bool:
        """
        On any message MetricsAggregator performs the following routine:

        1. Tries to clean up outdated raw metrics, because Datadog rejects
        metrics older than 4 hours.
        2. Gets all the keys of all the metrics with their timestamps.
        3. Groups these keys based on their timestamps.
        4. For every group - fetches raw metrics from a storage.
        5. Casts them into MergedMetric objects.
        6. Processes these MergedMetrics with a specified MetricsWrapper.
        7. Stores produced WrappedMetrics to a storage.
        8. Cleans up original raw metrics from a storage.

        Args:
            message: Anything.
        Return: Whether all the raw metrics were processed and stored.
        """
        logger.debug("[%s] Cleaning up outdated raw metrics.", self.name)
        self.redis = RedisStorage.get_instance()
        self.redis.ask(
            CleanupOutdatedRecords("metrics", ttl=self.metric_ttl, wrapped=False)
        )
        if not self.metrics_wrapper:
            return True

        keys = self.redis.ask(CollectKeys("metrics", wrapped=False))
        grouped_keys = MetricsMerger.group_metric_keys(keys, self.aggregate_interval)
        logger.info(
            "[%s] Separated %s metric keys into %s groups of %s seconds.",
            self.name,
            len(keys),
            len(grouped_keys),
            self.aggregate_interval,
        )

        return all(map(self._process_metrics, grouped_keys))

    def _process_metrics(self, keys: List[bytes]) -> bool:
        """
        Processes metrics.

        1. Fetches metrics from a storage by their keys.
        2. Merges them into a list of MergedMetric objects.
        3. Casts these MergedMetrics into WrappedMetrics using logic
        of a specified MetricsWrapper.
        4. Stores produced WrappedMetrics to a storage.
        5. Removes original raw metrics from a storage.

        Args:
            keys: List of metric keys to fetch data from a storage.
        Returns: Whether metrics were processed and cleaned up.
        """
        b_records = self.redis.ask(CollectValues("metrics", keys, wrapped=False))
        merged_metrics = MetricsMerger.merge_metrics(b_records, self.aggregate_interval)
        logger.info(
            "[%s] Merged %s raw metrics into %s Merged Metrics.",
            self.name,
            len(b_records),
            len(merged_metrics),
        )
        wrapped_metrics = self.metrics_wrapper.wrap_metrics(merged_metrics)
        logger.info(
            "[%s] Wrapped %s Merged Metrics into %s Wrapped Metrics.",
            self.name,
            len(merged_metrics),
            len(wrapped_metrics),
        )
        # Storing processed messages to a "wrapped" queue and cleanup:
        request = StoreRecords("metrics", wrapped_metrics, wrapped=True)
        metrics_stored = self.redis.ask(request)
        # Cleanup:
        if metrics_stored:
            cleaned_up = self.redis.ask(DeleteRecords("metrics", keys, wrapped=False))
        else:
            logger.warning(
                "[%s] Could not store %s Wrapped Metrics to a storage. "
                "Raw metrics are not cleaned.",
                self.name,
                len(wrapped_metrics),
            )
            cleaned_up = False
        if metrics_stored and not cleaned_up:
            logger.error(
                "[%s] Wrapped metrics were stored, but %s raw metrics "
                "were not cleaned up. Metrics can be duplicated!",
                self.name,
                len(b_records),
            )
        return metrics_stored and cleaned_up


class MetricsMerger:
    """
    MetricsMerger class is a part of a MergeAggregator workflow.

    It's responsible for separating raw metrics into groups and creating
    MergedMetric objects from raw data.
    Then these MergedMetric objects can be processed by a MetricsWrapper.
    """

    @staticmethod
    def group_metric_keys(
        metric_keys: List[Tuple[bytes, float]], interval: int
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
            metric_keys: List of tuples (key, timestamp).
            interval: Length of a `flush interval` for aggregation.
        Returns: List of lists with grouped metrics keys.
        """
        grouped = groupby(metric_keys, lambda record: record[1] // interval)
        result = [[keys for keys, t_stamps in group[1]] for group in grouped]
        return result

    @classmethod
    def merge_metrics(cls, b_records: List[bytes], interval: int) -> List[MergedMetric]:
        """
        Takes a list of bytes presumably representing JSON encoded raw
        metrics and tries to cast them to a list of MergedMetrics.

        1. List of bytes is casted into MergedMetric objects.
        2. MergedMetrics are being grouped by their unique type id.
        3. Grouped MergedMetrics are merged into a list of single MergedMetrics.

        Args:
            b_records: List of bytes objects representing raw metrics.
            interval: Flush interval value.
        Returns: List of MergedMetric objects.
        """
        single_metrics = filter(
            None, [cls._cast_to_metric(record, interval) for record in b_records]
        )
        grouped_metrics = groupby(single_metrics, lambda metric: metric.id)
        merged_metrics = [
            reduce(lambda a, b: a + b, metrics) for _, metrics in grouped_metrics
        ]
        return merged_metrics

    @staticmethod
    def _cast_to_metric(b_record: bytes, interval: int) -> Optional[MergedMetric]:
        """
        Gets a bytes object and tries to decode to a JSON it and cast it
        to a MergedMetric object.

        If it can't be casted for some reason, returns None.

        Args:
            b_record: Bytes object, presumably representing a raw metric.
            interval: Flush interval value.
        Return: MergedMetric object or None.
        """
        try:
            dict_metric = json.loads(b_record)
            merged_metric = MergedMetric(
                metric=dict_metric["metric"],
                type=dict_metric["type"],
                timestamps=[dict_metric["timestamp"]],
                values=[dict_metric["value"]],
                tags=dict_metric.get("tags"),
                interval=interval,
            )
        except (json.JSONDecodeError, TypeError, KeyError):
            return None
        return merged_metric
