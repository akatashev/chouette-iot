"""
MetricsSender actor.
"""
import json
import logging
import zlib
from typing import Any, List, Iterable

from chouette_iot_client import ChouetteClient  # type: ignore

from chouette_iot._sender import Sender
from chouette_iot.storage.messages import GetQueueSize

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
        On any message executes a process_records method for "metrics".

        Args:
            message: Can be anything.
        Returns: Whether data was dispatched and cleaned successfully.
        """
        return self.process_records("metrics")

    def add_global_tags(self, b_records: Iterable[bytes]) -> Iterable[dict]:
        """
        Takes a bytes objects that is expected to represent a JSON object,
        casts it to dict and adds global tags to it list of tags.

        Also it adds a "host" value if this value is specified.

        Args:
            b_records: Bytes objects representing metrics as JSON objects.
        Returns: Dicts representing metrics with updated tags.
        """
        for b_record in b_records:
            try:
                d_metric = json.loads(b_record)
            except (TypeError, json.JSONDecodeError):
                continue
            d_metric["tags"] = d_metric.get("tags", []) + self.tags
            if self.host:
                d_metric["host"] = self.host
            yield d_metric

    def dispatch_to_datadog(self, records: List[dict]) -> bool:
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
            records: List of prepared to dispatch metrics.
        Returns: Whether these metrics were accepted by Datadog.
        """
        # Send a 'chouette.queued.metrics' metric.
        if self.send_self_metrics:
            self.store_queue_size()
        series = json.dumps({"series": records})
        compressed_message: bytes = zlib.compress(series.encode())
        metrics_num = len(records)
        message_size = len(compressed_message)
        logger.info(
            "[%s] Dispatching %s metrics. Sending around %s KBs of data.",
            self.name,
            metrics_num,
            int(message_size / 1024),
        )
        dispatched = self._post_to_datadog(compressed_message, "v1/series")
        if dispatched and self.send_self_metrics:
            ChouetteClient.count("chouette.dispatched.metrics.number", metrics_num)
            ChouetteClient.count("chouette.dispatched.metrics.bytes", message_size)
        return dispatched

    def store_queue_size(self) -> None:
        """
        Calculates how many metrics are queued to be dispatched on this
        Sender run and stores this data as a raw metric.

        This is a 'gauge' type metric, because we care about the latest
        value and not about the possible sum of values.

        Returns: None.
        """
        # This one is Redis specific. Must be modified if other storage are implemented:
        size_request = GetQueueSize("metrics", wrapped=True)
        queue_size = self.storage.ask(size_request)
        if queue_size > 0:
            ChouetteClient.gauge("chouette.queued.metrics", queue_size)
