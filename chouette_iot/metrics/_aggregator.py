import logging
from typing import Any, List, Tuple

from chouette_iot import ChouetteConfig
from chouette_iot._singleton_actor import VitalActor
from chouette_iot.storages import StoragesFactory
from chouette_iot.storages.messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreRecords,
)
from ._merger import MetricsMerger
from ._metrics import WrappedMetric
from .wrappers import WrappersFactory

__all__ = ["MetricsAggregator"]

logger = logging.getLogger("chouette-iot")


class MetricsAggregator(VitalActor):
    """
    MetricsAggregator is an actor that is responsible for data aggregation.
    https://docs.datadoghq.com/developers/dogstatsd/data_aggregation/

    Environment variable `flush_interval` determines how often data is
    aggregated and it's analogous to a `flush interval` from DogStatsD
    documentation. Collected metrics are being processed in groups,
    where every group represents all the metrics emitted during
    `flush_interval` seconds.

    If Chouette was working constantly, it usually don't have lots of
    raw metrics to process, it's just some amount of metrics collected
    since the last Aggregation execution.
    However, if for some reason Chouette was stopped or down and other
    applications were producing metrics, grouping thousands of keys
    into small groups, gathering lots of groups of metrics and
    processing every group can take quite a while.

    To avoid duplicating metrics in this situation, MetricsAggregator
    intentionally has a blocking workflow based on `ask` patterns.

    If one aggregate call takes more than `flush_interval` to
    finish, other calls will be queued in the actor's mailbox and
    will be executed only when the first call is finished and processed
    metrics are cleaned up from a storage.
    """

    def __init__(self):
        super().__init__()
        config = ChouetteConfig()
        self.flush_interval = config.aggregate_interval
        self.ttl = config.metric_ttl
        self.metrics_wrapper = WrappersFactory.get_wrapper(config.metrics_wrapper)
        self.storage_type = config.storage_type
        self.storage = None

        if not self.metrics_wrapper:
            logger.warning(
                "[%s] No MetricsWrapper found. "
                "Raw metrics won't be collected and aggregated.",
                self.name,
            )

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
        self.storage = StoragesFactory.get_storage(self.storage_type)
        self._cleanup_outdated_raw_metrics(self.ttl)
        if not self.metrics_wrapper:
            return True

        keys_and_ts = self._collect_raw_keys_and_timestamps()
        grouped_keys = MetricsMerger.group_metric_keys(keys_and_ts, self.flush_interval)

        if keys_and_ts:
            logger.info(
                "[%s] Separated %s metric keys into %s groups of %s seconds.",
                self.name,
                len(keys_and_ts),
                len(grouped_keys),
                self.flush_interval,
            )

        processing_results = [self._process_metrics(keys) for keys in grouped_keys]
        return all(processing_results)

    def _cleanup_outdated_raw_metrics(self, ttl: int) -> bool:
        """
        Cleans up outdated metrics from the 'raw' metrics queue.
        
        Args:
            ttl: TTL of a metrics.
        Returns: Whether cleanup command was executed correctly.
        """
        cleanup_request = CleanupOutdatedRecords("metrics", ttl=ttl, wrapped=False)
        return self.storage.ask(cleanup_request)

    def _collect_raw_keys_and_timestamps(self) -> List[Tuple[bytes, float]]:
        """
        Collects all the metric keys from the 'raw' metrics queue with
        their timestamps.
        
        Returns: List of tuples (key, metric timestamp).
        """
        collect_keys_request = CollectKeys("metrics", wrapped=False)
        return self.storage.ask(collect_keys_request)

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
        records = self._collect_raw_records(keys)
        merged_metrics = MetricsMerger.merge_metrics(records, self.flush_interval)
        logger.info(
            "[%s] Merged %s raw metrics into %s Merged Metrics.",
            self.name,
            len(records),
            len(merged_metrics),
        )
        wrapped_metrics = self.metrics_wrapper.wrap_metrics(merged_metrics)
        logger.info(
            "[%s] Wrapped %s Merged Metrics into %s Wrapped Metrics.",
            self.name,
            len(merged_metrics),
            len(wrapped_metrics),
        )
        metrics_stored = self._store_wrapped_metrics(wrapped_metrics)
        if not metrics_stored:
            logger.warning(
                "[%s] Could not store %s Wrapped Metrics to a storage. "
                "Raw metrics are not cleaned.",
                self.name,
                len(wrapped_metrics),
            )
            return False
        cleaned_up = self._delete_raw_records(keys)
        if not cleaned_up:
            logger.error(
                "[%s] Wrapped metrics were stored, but %s raw metrics "
                "were not cleaned up. Metrics can be duplicated!",
                self.name,
                len(records),
            )
        return metrics_stored and cleaned_up

    def _collect_raw_records(self, keys: List[bytes]) -> List[bytes]:
        """
        Collects bytes records from the 'raw' metrics queue.

        Args:
            keys: Keys of records to collect.
        Returns: List of bytes, presumably with metrics.
        """
        collect_records_request = CollectValues("metrics", keys, wrapped=False)
        return self.storage.ask(collect_records_request)

    def _store_wrapped_metrics(self, metrics: List[WrappedMetric]) -> bool:
        """
        Stores wrapped metrics to the 'wrapped' metrics queue.

        Args:
            metrics: List of WrappedMetric objects.
        Returns: Whether storing was executed successfully.
        """
        store_request = StoreRecords("metrics", metrics, wrapped=True)
        return self.storage.ask(store_request)

    def _delete_raw_records(self, keys: List[bytes]) -> bool:
        """
        Deletes raw metrics from the 'raw' metrics queue.

        Args:
            keys: List of keys for a cleanup.
        Returns: Whether removal operation was successful.
        """
        delete_request = DeleteRecords("metrics", keys, wrapped=False)
        return self.storage.ask(delete_request)
