import json
import logging
from functools import reduce
from itertools import groupby
from typing import Optional, List

from pykka.gevent import GeventActor

from chouette import ChouetteConfig, get_redis_handler
from chouette._messages import CollectKeys, CollectValues, DeleteRecords, StoreMetrics

logger = logging.getLogger("chouette")

__all__ = ["MetricsAggregator"]


class MetricsAggregator(GeventActor):
    def __init__(self):
        super().__init__()
        config = ChouetteConfig()
        self.aggregate_interval = config.interval_aggregate

    def on_receive(self, message):
        redis_handler = get_redis_handler()
        keys = redis_handler.ask(CollectKeys("metrics"))
        grouped_keys = MetricsMerger.group_metric_keys(keys, self.aggregate_interval)

        for keys_group in grouped_keys:
            # Getting actual records from Redis and processing them:
            records = redis_handler.ask(CollectValues("metrics", keys_group))
            merged_records = MetricsMerger.merge_metrics(records)
            wrapped_records = MetricsWrapper.wrap_metrics(merged_records)
            # Storing processed messages to a "wrapped" queue and cleanup:
            values_stored = redis_handler.ask(StoreMetrics(wrapped_records))
            if values_stored:
                cleaned_up = redis_handler.ask(DeleteRecords("metrics", keys_group))
                # Todo: Log something.


class MetricsMerger:
    @staticmethod
    def group_metric_keys(metric_keys, aggregate_interval) -> List[List[str]]:
        grouped_keys = []
        for _, grouped_keys_and_timestamps in groupby(
            metric_keys, lambda key_and_ts: key_and_ts[1] // aggregate_interval
        ):
            keys_group = [keys for (keys, _) in grouped_keys_and_timestamps]
            grouped_keys.append(keys_group)
        return grouped_keys

    @classmethod
    def cast_metrics_to_dicts(cls, metrics) -> list:
        casted_metrics = [cls.get_metric_dict(record) for record in metrics]
        metrics = list(filter(None, casted_metrics))
        return metrics

    @staticmethod
    def get_metric_dict(record) -> Optional[dict]:
        try:
            metric_dict = json.loads(record)
        except json.JSONDecodeError:
            metric_dict = None
        return metric_dict

    @staticmethod
    def get_tags_list(metric) -> list:
        tags = metric.get("tags")
        if not tags or not isinstance(tags, dict):
            return []
        tags_list = [f"{name}:{str(value)}" for name, value in tags]
        return sorted(tags_list)

    @classmethod
    def merge_metrics(cls, metrics_dicts) -> dict:
        """
        Takes a list of dicts containing metrics. For every element of this dict it concatenate this dict's
        tags and creates a resulting key using a template "{metric_name}_{metric_type}_{tags}".
        This key is used as a dict key for dict instance:
        {"values": list, "timestamps": list, "name": str, "type": str, "tags" (optional): dict}.
        Value and timestamp of every metric with this key is added to the corresponding lists.
        :param metrics_dicts: A list of dicts containing OWL metrics.
        :return: A dict with these metrics merged under a key described above.
        """
        data = {}
        for metric in metrics_dicts:
            tags_list = cls.get_tags_list(metric)
            if tags_list:
                metric["tags"] = tags_list
            tags_string = "_".join(tags_list)
            key = f"{metric['name']}_{metric.get('type')}_{tags_string}"
            if key not in data:
                data[key] = cls.generate_merged_structure(metric)

            data[key]["values"].append(metric["value"])
            data[key]["timestamps"].append(metric["timestamp"])
        return data

    @staticmethod
    def generate_merged_structure(metric):
        structure = {
            "values": [],
            "timestamps": [],
            "name": metric["name"],
            "type": metric["type"],
        }
        if "tags" in metric:
            structure["tags"] = metric["tags"]
        return structure


class MetricsWrapper:
    @classmethod
    def wrap_metrics(cls, merged_metrics_dict) -> list:
        wrapped_metrics = []
        for metric_type in merged_metrics_dict:
            raw_metric = merged_metrics_dict[metric_type]
            wrapped_metric = {
                "metric": raw_metric["name"],
                "tags": raw_metric["tags"],
                "type": raw_metric["type"],
            }
            try:
                points = cls.calculate_metric_points(raw_metric)
            except TypeError:
                continue
            # Add wrapped metric to the list of wrapped metrics:
            wrapped_metric["points"] = [points]
            wrapped_metrics.append(wrapped_metric)
            # Add an additional count metric for every gauge metric:
            if raw_metric["type"] == "gauge":
                timestamp = points[0]
                count_metric = {
                    "metric": f"{raw_metric['name']}.count",
                    "tags": raw_metric["tags"],
                    "points": [[timestamp, len(raw_metric["values"])]],
                    "type": "count",
                }
                wrapped_metrics.append(count_metric)
        return wrapped_metrics

    @staticmethod
    def calculate_metric_points(raw_metric) -> list:
        values = raw_metric["type"]
        value = reduce(lambda x, y: x + y, values)
        if raw_metric["type"] != "count":
            value = value / len(values)
        return [max(raw_metric["timestamps"]), value]
