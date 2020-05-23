import json
import logging
import sys
import zlib
from typing import Any, List, Optional

import requests
from requests.exceptions import RequestException

from chouette import ChouetteConfig
from chouette._singleton_actor import SingletonActor
from chouette.storages import RedisStorage
from chouette.storages.messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
)

__all__ = ["MetricsSender"]

logger = logging.getLogger("chouette")


class MetricsSender(SingletonActor):
    def on_failure(self, exception_type, exception_value, traceback):
        print(f"{exception_type}: {exception_value}")

    def __init__(self):
        """
        On creation MetricsCollector reads a list of its plugins from
        environment variables.
        """
        super().__init__()
        config = ChouetteConfig()
        self.api_key = config.api_key
        self.bulk_size = config.metrics_bulk_size
        self.datadog_url = config.datadog_url
        self.metric_ttl = config.metric_ttl
        self.redis = None
        self.send_self_metrics = config.send_self_metrics
        self.tags = config.global_tags
        self.timeout = config.release_interval * 0.8

    def on_receive(self, message: Any) -> bool:
        self.redis = RedisStorage.get_instance()
        self.redis.ask(CleanupOutdatedRecords("metrics", self.metric_ttl))
        keys = self._collect_keys()
        if not keys:
            return False

        metrics = self._collect_metrics(keys)

        dispatched = self._dispatch_to_datadog(metrics)
        if dispatched:
            cleaned_up = self.redis.ask(DeleteRecords("metrics", keys, wrapped=True))
        else:
            cleaned_up = False

        return dispatched and cleaned_up

    def _bytes_to_json(self, b_metric: bytes) -> Optional[str]:
        try:
            d_metric = json.loads(b_metric)
        except (TypeError, json.JSONDecodeError):
            return None
        d_metric["tags"] = d_metric.get("tags", []) + self.tags

        return json.dumps(d_metric)

    def _collect_keys(self):
        request = CollectKeys("metrics", amount=self.bulk_size, wrapped=True)
        return self.redis.ask(request)

    def _collect_metrics(self, keys: List[bytes]):
        b_metrics = self.redis.ask(CollectValues("metrics", keys, wrapped=True))
        return list(filter(None, map(self._bytes_to_json, b_metrics)))

    def _dispatch_to_datadog(self, metrics: List[str]) -> bool:
        series = json.dumps({"series": metrics})
        compressed_message = zlib.compress(series.encode())

        try:
            dd_response = requests.post(
                f"{self.datadog_url}/v1/series",
                params={"api_key": self.api_key},
                data=compressed_message,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "deflate",
                },
                timeout=self.timeout,
            )
            if not dd_response.status_code == 202:
                logger.error(
                    "Unexpected response [%s]: %s",
                    dd_response.status_code,
                    dd_response.text,
                )
                return False
        except RequestException:
            logger.error("HTTP interaction with %s was unsuccessful.", self.datadog_url)
            return False

        if self.send_self_metrics:
            message_size = sys.getsizeof(compressed_message)
            metrics_number = len(metrics)
            # Todo: Send internal metrics.
        return True
