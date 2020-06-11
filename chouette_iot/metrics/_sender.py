"""
MetricsSender actor.
"""
import json
import logging
import zlib
from typing import Any, List, Optional

import requests
from requests.exceptions import RequestException

from chouette_iot import ChouetteConfig
from chouette_iot._singleton_actor import VitalActor
from chouette_iot.metrics import RawMetric
from chouette_iot.storages import RedisStorage
from chouette_iot.storages.messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreRecords,
)
from chouette_iot.storages._redis_messages import GetHashSizes

__all__ = ["MetricsSender"]

logger = logging.getLogger("chouette-iot")


class MetricsSender(VitalActor):
    """
    MetricsSender is an actor that communicates to Datadog.

    Its responsibility is to cleanup outdated metrics, gather actual
    metrics, compress them and dispatch to Datadog API.
    """

    def __init__(self):
        """
        Next configuration is being extracted from chouette_iotConfig:

        * api_key: Datadog API key.
        * bulk_size: How many metrics we want to send in one bulk at max.
            Default value is 10000. Size of compressed chunk of 10000 metrics
            is around 150KBs. Increase it if your device connection is fast.
        * datadog_url: Datadog URL. It has a default value.
        * metric_ttl: Datadog drops outdated metric, so we clean them before
            sending data. This option says how many seconds is considered
            being "outdated". Metrics older than TTL are being dropped.
        * tags: List of global tags to add to every metric. Should have
            something that gives you a chance to understand what device
            send this metrics. E.g.: a 'host' tag.
        * timeout: Maximum HTTPS Request Timeout for a metrics dispatch
            request.
        """
        super().__init__()
        config = ChouetteConfig()
        self.api_key = config.api_key
        self.bulk_size = config.metrics_bulk_size
        self.datadog_url = config.datadog_url
        self.metric_ttl = config.metric_ttl
        self.redis = RedisStorage.get_instance()
        self.send_self_metrics = config.send_self_metrics
        self.tags = config.global_tags
        self.timeout = int(config.release_interval * 0.8)

    def on_receive(self, message: Any) -> bool:
        """
        On any message MetricsSender:

        1. Performs outdated metrics cleanup prior to gathering data.
        2. Gets a bulk of keys from a RedisStorage actor.
        3. Collects metrics and adds global tags to every of them.
        4. Tries to dispatch them as a compressed "series" message.
        5. If they were dispatched successfully - deletes data from Redis.

        To preserve the exact order of actions, MetricsSender intentially
        communicates to RedisStorage in a blocking manner, via `ask` requests.

        Args:
            message: Can be anything.
        Returns: Whether data was dispatched and cleaned successfully.
        """
        logger.debug("[%s] Cleaning up outdated wrapped metrics.", self.name)
        self.redis = RedisStorage.get_instance()
        self.redis.ask(
            CleanupOutdatedRecords("metrics", ttl=self.metric_ttl, wrapped=True)
        )
        keys = self.collect_keys()
        if not keys:
            logger.info("[%s] Nothing to dispatch.", self.name)
            return True
        metrics = self.collect_metrics(keys)
        dispatched = self.dispatch_to_datadog(metrics)
        if dispatched:
            cleaned_up = self.redis.ask(DeleteRecords("metrics", keys, wrapped=True))
            if not cleaned_up:
                logger.error(
                    "[%s] Metrics were dispatched, but not cleaned up!", self.name
                )
        else:
            logger.warning(
                "[%s] Metrics were neither dispatched, nor cleaned.", self.name
            )
            cleaned_up = False

        return dispatched and cleaned_up

    def add_global_tags(self, b_metric: bytes) -> Optional[dict]:
        """
        Takes a bytes objects that is expected to represent a JSON object,
        casts it to dict and adds global tags to it list of tags.

        Args:
            b_metric: Bytes object representing a metric as a JSON object.
        Returns: Dict representing a metric with updated tags..
        """
        try:
            d_metric = json.loads(b_metric)
        except (TypeError, json.JSONDecodeError):
            return None
        d_metric["tags"] = d_metric.get("tags", []) + self.tags
        return d_metric

    def collect_keys(self) -> List[bytes]:
        """
        Requests a `self.bulk_size` amount of wrapped metrics keys from Redis.

        It returns the oldest keys to retrieve as much data as possible in case
        of a networking outage.

        Returns: List of metrics keys as bytes.
        """
        request = CollectKeys("metrics", amount=self.bulk_size, wrapped=True)
        keys_and_ts = self.redis.ask(request)
        logger.debug("[%s] Collected %s keys.", self.name, len(keys_and_ts))
        return list(map(lambda pair: pair[0], keys_and_ts))

    def collect_metrics(self, keys: List[bytes]) -> List[dict]:
        """
        Gets a list of metrics from Redis, adds global tags to them and prepare
        them to be dispatched to Datadog.

        Args:
            keys: List of metrics keys as bytes.
        Returns: List of prepared to dispatch metrics.
        """
        b_metrics = self.redis.ask(CollectValues("metrics", keys, wrapped=True))
        logger.debug("[%s] Collected %s metrics.", self.name, len(b_metrics))
        return list(filter(None, map(self.add_global_tags, b_metrics)))

    def dispatch_to_datadog(self, metrics: List[dict]) -> bool:
        """
        Dispatches metrics to Datadog as a "series" POST request.

        https://docs.datadoghq.com/api/v1/metrics/#submit-metrics

        1. It takes the list of prepared metrics.
        2. Casts it to a single "series" request.
        3. Compresses it.
        4. Tries to send it to Datadog.
        5. If it's expected to send self metrics, it sends a number
           of dispatched metrics and the size in bytes of a message.

        Args:
            metrics: List of prepared to dispatch metrics.
        Returns: Whether these metrics were accepted by Datadog.
        """
        series = json.dumps({"series": metrics})
        compressed_message: bytes = zlib.compress(series.encode())
        metrics_num = len(metrics)
        message_size = len(compressed_message)
        logger.info(
            "[%s] Dispatching %s metrics. Sending around %s KBs of data.",
            self.name,
            metrics_num,
            int(message_size / 1024),
        )
        dispatched = self._post_to_datadog(compressed_message)
        if not dispatched:
            return False
        if self.send_self_metrics:
            self._send_self_metrics(metrics_num, message_size)
        return True

    def _post_to_datadog(self, message: bytes) -> bool:
        """
        Implements actual HTTPS interaction with Datadog.

        On message 202 Accepted returns True, on any other message or
        RequestsException returns False and logs an error message.

        Arg:
            message: Compressed message to sent.
        Return: Bool that shows whether the message was accepted.
        """
        try:
            dd_response = requests.post(
                f"{self.datadog_url}/v1/series",
                params={"api_key": self.api_key},
                data=message,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "deflate",
                },
                timeout=self.timeout,
            )
            if not dd_response.status_code == 202:
                logger.error(
                    "[%s] Unexpected response from Datadog: %s: %s",
                    self.name,
                    dd_response.status_code,
                    dd_response.text,
                )
                return False
        except (RequestException, IOError) as error:
            logger.error(
                "[%s] Could not dispatch metrics due to a HTTP error: %s",
                self.datadog_url,
                error,
            )
            return False
        return True

    def _send_self_metrics(self, metrics_num: int, message_size: int) -> None:
        """
        Stores data about how many metrics were sent to Datadog, their size
        in bytes and amount of messages to send in the wrapped metrics queue
        if there are any metrics not sent during this Chouette run.

        Args:
            metrics_num: Number of dispatched metrics.
            message_size: Size of a sent message in bytes.
        Returns: None.
        """
        self_metrics = [
            RawMetric(
                metric="chouette.dispatched.metrics.number",
                type="count",
                value=metrics_num,
            ),
            RawMetric(
                metric="chouette.dispatched.metrics.bytes",
                type="count",
                value=message_size,
            ),
        ]
        # Since Redis is the only broker, this is valid for now.
        # Not sure whether this data is really useful, will review it later.
        wrapped_queue = self.redis.ask(
            GetHashSizes(["chouette:metrics:wrapped.values"])
        )
        if wrapped_queue:
            ready_to_dispatch = wrapped_queue.pop()[1] - metrics_num
            if ready_to_dispatch > 0:
                self_metrics.append(
                    RawMetric(
                        metric="chouette.queued.metrics",
                        type="gauge",
                        value=ready_to_dispatch,
                    )
                )
        self.redis.tell(StoreRecords("metrics", self_metrics, wrapped=False))
