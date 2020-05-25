import json
import logging
from collections import defaultdict
from functools import reduce
from itertools import groupby
from typing import Any, Iterator, List, Optional, Tuple, Union

from chouette import ChouetteConfig
from chouette._singleton_actor import SingletonActor
from chouette.metrics import MergedMetric, RawMetric
from chouette.metrics.wrappers import WrappersFactory
from chouette.storages import RedisStorage
from chouette.storages.messages import (
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreRecords,
)

__all__ = ["MetricsAggregator"]

logger = logging.getLogger("chouette")


class MetricsAggregator(SingletonActor):
    def __init__(self):
        super().__init__()
        config = ChouetteConfig()
        self.aggregate_interval = config.aggregate_interval
        wrapper_name = config.metrics_wrapper
        self.metrics_wrapper = WrappersFactory.get_wrapper(wrapper_name)
        self.redis = None

    def on_receive(self, message: Any) -> bool:
        print("Aggregator triggered.")
        if not self.metrics_wrapper:
            logger.warning(
                "No MetricsWrapper found. "
                "Raw metrics are not collected and aggregated."
            )
            return False
        self.redis = RedisStorage.get_instance()

        keys = self.redis.ask(CollectKeys("metrics", wrapped=False))
        grouped_keys = MetricsMerger.group_metric_keys(keys, self.aggregate_interval)

        return all(map(self._process_keys_group, grouped_keys))

    def _process_keys_group(self, keys: List[bytes]) -> bool:
        # Getting actual records from Redis and processing them:
        b_metrics = self.redis.ask(CollectValues("metrics", keys, wrapped=False))
        merged_records = MetricsMerger.merge_metrics(b_metrics)
        wrapped_records = self.metrics_wrapper.wrap_metrics(merged_records)
        # Storing processed messages to a "wrapped" queue and cleanup:
        request = StoreRecords("metrics", wrapped_records, wrapped=True)
        values_stored = self.redis.ask(request)
        # Cleanup:
        if values_stored:
            cleaned_up = self.redis.ask(DeleteRecords("metrics", keys, wrapped=False))
        else:
            cleaned_up = False

        return values_stored and cleaned_up


class MetricsMerger:
    @staticmethod
    def group_metric_keys(
        metric_keys: List[Tuple[bytes, float]], interval: int
    ) -> Iterator[List[bytes]]:
        grouped = groupby(metric_keys, lambda record: record[1] // interval)
        keys = map(lambda group: list(map(lambda pair: pair[0], group[1])), grouped)
        return keys

    @classmethod
    def merge_metrics(cls, b_metrics: List[bytes]) -> List[MergedMetric]:
        single_metrics = cls._cast_bytes_to_metrics(b_metrics)
        grouped_metrics = groupby(
            single_metrics,
            key=lambda metric: f"{metric.name}_{metric.type}_{'_'.join(metric.tags)}",
        )
        merged_metrics = map(
            lambda group: reduce(lambda a, b: a + b, group), grouped_metrics
        )
        return list(merged_metrics)

    @classmethod
    def _cast_bytes_to_metrics(cls, b_metrics: List[bytes]) -> Iterator:
        cast_metrics = map(cls._get_metric, b_metrics)
        metrics = filter(None, cast_metrics)
        return metrics

    @staticmethod
    def _get_metric(b_metric: bytes) -> Optional[MergedMetric]:
        try:
            merged_metric = RawMetric(**json.loads(b_metric)).mergify()
        except (json.JSONDecodeError, TypeError, KeyError):
            merged_metric = None
        return merged_metric
