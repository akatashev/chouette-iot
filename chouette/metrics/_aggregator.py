import json
import logging
from collections import defaultdict
from functools import reduce
from itertools import groupby
from typing import Any, Iterator, List, Optional, Tuple, Union

from pykka import ActorRef
from pykka.gevent import GeventActor

from chouette import ChouetteConfig, get_redis_handler
from chouette.messages import CollectKeys, CollectValues, DeleteRecords, StoreMetrics
from chouette.metrics import MergedMetric, WrappedMetric

logger = logging.getLogger("chouette")

__all__ = ["MetricsAggregator"]


class MetricsAggregator(GeventActor):
    def __init__(self):
        super().__init__()
        config = ChouetteConfig()
        self.aggregate_interval = config.interval_aggregate

    def on_receive(self, message: Any) -> None:
        redis_handler = get_redis_handler()

        keys = redis_handler.ask(CollectKeys("metrics"))
        grouped_keys = MetricsMerger.group_metric_keys(keys, self.aggregate_interval)

        for keys_group in grouped_keys:
            stored, cleaned = self._process_keys_group(keys_group, redis_handler)

    @staticmethod
    def _process_keys_group(
        keys: List[Union[str, bytes]], redis_handler: ActorRef
    ) -> Tuple[bool, bool]:
        # Getting actual records from Redis and processing them:
        records = redis_handler.ask(CollectValues("metrics", keys))
        merged_records = MetricsMerger.merge_metrics(records)
        wrapped_records = MetricsWrapper.wrap_metrics(merged_records)
        # Storing processed messages to a "wrapped" queue and cleanup:
        values_stored = redis_handler.ask(StoreMetrics(wrapped_records))
        if values_stored:
            cleaned_up = redis_handler.ask(DeleteRecords("metrics", keys))
        else:
            cleaned_up = False
        return values_stored, cleaned_up


class MetricsMerger:
    @staticmethod
    def group_metric_keys(
        metric_keys: List[Tuple[Union[bytes, str], float]], interval: int
    ) -> Iterator:
        grouped = groupby(metric_keys, lambda record: record[1] // interval)
        keys = map(lambda group: list(map(lambda pair: pair[0], group[1])), grouped)
        return keys

    @classmethod
    def merge_metrics(cls, metrics: List[bytes]) -> List[MergedMetric]:
        dicts = cls._cast_metrics_to_dicts(metrics)
        merged_metrics = map(lambda m: cls._produce_merged_metric(m), dicts)

        buffer = defaultdict(list)
        for key, metric in merged_metrics:
            buffer[key].append(metric)
        result = {k: reduce(lambda a, b: a + b, metric) for k, metric in buffer.items()}
        return list(result.values())

    @classmethod
    def _cast_metrics_to_dicts(cls, metrics) -> Iterator:
        casted_metrics = map(lambda record: cls._get_metric_dict(record), metrics)
        metrics = filter(None, casted_metrics)
        return metrics

    @staticmethod
    def _get_metric_dict(record) -> Optional[dict]:
        try:
            metric_dict = json.loads(record)
        except (json.JSONDecodeError, TypeError):
            metric_dict = None
        return metric_dict

    @classmethod
    def _produce_merged_metric(cls, metric: dict) -> Tuple[str, MergedMetric]:
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


class MetricsWrapper:
    """
    Todo: Rewrite this class to support correct metric types.
    """

    @classmethod
    def wrap_metrics(cls, merged_metrics):
        if not merged_metrics:
            return []
        wrapped_metrics = map(cls._wrap_metric, merged_metrics)
        return reduce(lambda a, b: a + b, wrapped_metrics)

    @classmethod
    def _wrap_metric(cls, merged_metric):
        try:
            timestamp, value = cls._calculate_metric_points(merged_metric)
        except TypeError:
            return []

        metrics = [
            WrappedMetric(
                metric=merged_metric.name,
                metric_type=merged_metric.type,
                timestamp=timestamp,
                value=value,
                tags=merged_metric.tags,
            )
        ]
        if merged_metric.type == "gauge":
            metrics.append(
                WrappedMetric(
                    metric=f"{merged_metric.name}.count",
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
