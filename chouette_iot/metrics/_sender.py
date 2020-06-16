"""
MetricsSender actor.
"""
import json
import logging
import zlib
from typing import Any, List, Optional

from chouette_iot_client import ChouetteClient  # type: ignore

from chouette_iot._sender import Sender
from chouette_iot.storages import RedisStorage
from chouette_iot.storages._redis_messages import GetHashSizes
from chouette_iot.storages.messages import (
    CleanupOutdatedRecords,
    DeleteRecords,
)

__all__ = ["MetricsSender"]

logger = logging.getLogger("chouette-iot")


class MetricsSender(Sender):
    """
    MetricsSender is an actor that communicates to Datadog.

    Its responsibility is to cleanup outdated metrics, gather actual
    metrics, compress them and dispatch to Datadog API.
    """

    def __init__(self):
        """
        Next configuration is being extracted from ChouetteConfig:

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
            send these metrics.
        * timeout: Maximum HTTPS Request Timeout for a metrics dispatch
            request.
        """
        super().__init__()
        self.bulk_size = self.config.metrics_bulk_size
        self.ttl = self.config.metric_ttl

    def on_receive(self, message: Any) -> bool:
        """
        On any message MetricsSender:

        1. Performs outdated metrics cleanup prior to gathering data.
        2. Gets a bulk of keys from a RedisStorage actor.
        3. Collects metrics and adds global tags to every of them.
        4. Tries to dispatch them as a compressed "series" message.
        5. If they were dispatched successfully - deletes data from Redis.

        To preserve the exact order of actions, MetricsSender intentionally
        communicates to RedisStorage in a blocking manner, via `ask` requests.

        Args:
            message: Can be anything.
        Returns: Whether data was dispatched and cleaned successfully.
        """
        logger.debug("[%s] Cleaning up outdated wrapped metrics.", self.name)
        self.redis = RedisStorage.get_instance()
        self.redis.ask(
            CleanupOutdatedRecords("metrics", ttl=self.ttl, wrapped=True)
        )
        keys = self.collect_keys("metrics")
        if not keys:
            logger.info("[%s] Nothing to dispatch.", self.name)
            return True
        metrics = self.collect_records(keys, "metrics")
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

        Also it adds a "host" value if this value is specified.

        Args:
            b_metric: Bytes object representing a metric as a JSON object.
        Returns: Dict representing a metric with updated tags.
        """
        try:
            d_metric = json.loads(b_metric)
        except (TypeError, json.JSONDecodeError):
            return None
        d_metric["tags"] = d_metric.get("tags", []) + self.tags
        if self.host:
            d_metric["host"] = self.host
        return d_metric

    def dispatch_to_datadog(self, metrics: List[dict]) -> bool:
        """
        Dispatches metrics to Datadog as a "series" POST request.

        https://docs.datadoghq.com/api/v1/metrics/#submit-metrics

        1. It takes the list of prepared metrics.
        2. Casts it to a single "series" request.
        3. Compresses it.
        4. Tries to send it to Datadog.

        If Chouette is expected to send self metrics, as a side
        effect, this function sends 3 metrics:
        1. How many messages are queued to be dispatched this
        minute.
        2. How many metrics were sent (if they were sent).
        3. How many bytes were sent (if they were sent).

        Args:
            metrics: List of prepared to dispatch metrics.
        Returns: Whether these metrics were accepted by Datadog.
        """
        # Send a 'chouette.queued.metrics' metric.
        if self.send_self_metrics:
            self.store_queue_size()
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
        dispatched = self._post_to_datadog(compressed_message, "v1/series")
        if not dispatched:
            return False
        if self.send_self_metrics:
            ChouetteClient.count("chouette.dispatched.metrics.number", metrics_num)
            ChouetteClient.count("chouette.dispatched.metrics.bytes", message_size)
        return True

    def store_queue_size(self) -> None:
        """
        Calculates how many metrics are queued to be dispatched on this
        Sender run and stores this data as a raw metric.

        This is a 'gauge' type metric, because we care about the latest
        value and not about the possible sum of values.

        Returns: None.
        """
        queues_sizes = self.redis.ask(GetHashSizes(["chouette:metrics:wrapped.values"]))
        if queues_sizes:
            _, metrics_queue_size = queues_sizes.pop()
            ChouetteClient.gauge("chouette.queued.metrics", metrics_queue_size)
