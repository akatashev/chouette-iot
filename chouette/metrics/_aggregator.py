import json
import logging
from collections import defaultdict
from functools import reduce
from itertools import groupby
from typing import Any, Iterator, List, Optional, Tuple, Union

from chouette import ChouetteConfig
from chouette._singleton_actor import SingletonActor
from chouette.metrics import MergedMetric
from chouette.metrics.wrappers import WrappersFactory
from chouette.storages import RedisStorage
from chouette.storages.messages import (
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreRecords,
)

logger = logging.getLogger("chouette")

__all__ = ["MetricsAggregator"]


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

        keys = self.redis.ask(CollectKeys("metrics"))
        grouped_keys = MetricsMerger.group_metric_keys(keys, self.aggregate_interval)

        return all(map(self._process_keys_group, grouped_keys))

    def _process_keys_group(self, keys: List[Union[str, bytes]]) -> bool:
        # Getting actual records from Redis and processing them:
        b_metrics = self.redis.ask(CollectValues("metrics", keys))
        merged_records = MetricsMerger.merge_metrics(b_metrics)
        wrapped_records = self.metrics_wrapper.wrap_metrics(merged_records)
        # Storing processed messages to a "wrapped" queue and cleanup:
        request = StoreRecords("metrics", wrapped_records, wrapped=True)
        values_stored = self.redis.ask(request)
        # Cleanup:
        if values_stored:
            cleaned_up = self.redis.ask(DeleteRecords("metrics", keys))
        else:
            cleaned_up = False

        return values_stored and cleaned_up


class MetricsMerger:
    @staticmethod
    def group_metric_keys(
        metric_keys: List[Tuple[Union[bytes, str], float]], interval: int
    ) -> Iterator:
        grouped = groupby(metric_keys, lambda record: record[1] // interval)
        keys = map(lambda group: list(map(lambda pair: pair[0], group[1])), grouped)
        return keys

    @classmethod
    def merge_metrics(cls, b_metrics: List[bytes]) -> List[MergedMetric]:
        d_metrics = cls._cast_metrics_to_dicts(b_metrics)
        key_metric_pair = map(cls._get_merged_metric_pair, d_metrics)

        buffer = defaultdict(list)
        for key, metric in key_metric_pair:
            buffer[key].append(metric)

        merged_metrics = {
            key: reduce(lambda a, b: a + b, metric) for key, metric in buffer.items()
        }.values()
        return list(merged_metrics)

    @classmethod
    def _cast_metrics_to_dicts(cls, b_metrics: List[bytes]) -> Iterator:
        casted_metrics = map(cls._get_metric_dict, b_metrics)
        metrics = filter(None, casted_metrics)
        return metrics

    @staticmethod
    def _get_metric_dict(b_metric: bytes) -> Optional[dict]:
        try:
            d_metric = json.loads(b_metric)
        except (json.JSONDecodeError, TypeError):
            d_metric = None
        return d_metric

    @classmethod
    def _get_merged_metric_pair(cls, metric: dict) -> Tuple[str, MergedMetric]:
        metric["tags"] = cls._get_tags_list(metric)
        tags_string = "_".join(metric["tags"])
        key = f"{metric['name']}_{metric.get('type')}_{tags_string}"
        merged_metric = MergedMetric(
            name=metric["name"],
            metric_type=metric.get("type"),
            values=[metric["value"]],
            timestamps=[metric["timestamp"]],
            tags=metric["tags"],
        )
        return key, merged_metric

    @staticmethod
    def _get_tags_list(metric: dict) -> list:
        tags = metric.get("tags")
        try:
            tags_list = [f"{name}:{str(value)}" for name, value in tags]
        except TypeError:
            tags_list = []
        return sorted(tags_list)
