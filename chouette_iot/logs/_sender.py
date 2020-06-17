"""
LogsSender actor.
"""
import json
import logging
import zlib
from typing import Any, List, Optional

from chouette_iot_client import ChouetteClient  # type: ignore

from chouette_iot._sender import Sender

__all__ = ["LogsSender"]

logger = logging.getLogger("chouette-iot")


class LogsSender(Sender):
    """
    LogsSender is an actor that communicates to Datadog.

    Its responsibility is to cleanup outdated log records, gather actual
    records, compress them and dispatch to Datadog API.
    """

    def __init__(self):
        """
        Next configuration is being extracted from ChouetteConfig:

        * api_key: Datadog API key.
        * datadog_url: Datadog URL. It has a default value 'datadog_logs_url'.
        * log_ttl: Datadog drops outdated logs, so we clean them before
            sending data. This option says how many seconds is considered
            being "outdated". Logs older than TTL are being dropped.
        * tags: List of global tags to add to every log. Should have
            something that gives you a chance to understand what device
            send these logs.
        * timeout: Maximum HTTPS Request Timeout for a logs dispatch
            request.
        """
        super().__init__()
        self.bulk_size = 500
        self.datadog_url = self.config.datadog_logs_url
        self.ttl = self.config.log_ttl

    def on_receive(self, message: Any) -> bool:
        """
        On any message executes a process_records method for "logs".

        Args:
            message: Can be anything.
        Returns: Whether data was dispatched and cleaned successfully.
        """
        return self.process_records("logs")

    def add_global_tags(self, b_log: bytes) -> Optional[dict]:
        """
        Takes a bytes objects that is expected to represent a JSON object,
        casts it to dict and adds global tags to it list of tags.

        Also it adds a "host" value if this value is specified.

        Args:
            b_log: Bytes object representing a log as a JSON object.
        Returns: Dict representing a log with updated tags.
        """
        try:
            d_log = json.loads(b_log)
        except (TypeError, json.JSONDecodeError):
            return None
        tags = d_log.get("ddtags", []) + self.tags
        d_log["ddtags"] = ",".join(tags)
        if self.host:
            d_log["host"] = self.host
        return d_log

    def dispatch_to_datadog(self, logs: List[dict]) -> bool:
        """
        Dispatches logs to Datadog:

        https://docs.datadoghq.com/api/v1/logs/#send-logs

        1. It takes the list of prepared logs.
        3. Compresses it.
        4. Tries to send it to Datadog.

        If Chouette is expected to send self metrics, as a side
        effect, this function sends 2 metrics:
        2. How many log records were sent (if they were sent).
        3. How many bytes were sent (if they were sent).

        Args:
            logs: List of prepared to dispatch logs.
        Returns: Whether these logs were accepted by Datadog.
        """
        compressed_message: bytes = zlib.compress(json.dumps(logs).encode())
        logs_num = len(logs)
        message_size = len(compressed_message)
        logger.info(
            "[%s] Dispatching %s logss. Sending around %s KBs of data.",
            self.name,
            logs_num,
            int(message_size / 1024),
        )
        dispatched = self._post_to_datadog(compressed_message, "v1/input")
        if not dispatched:
            return False
        if self.send_self_metrics:
            ChouetteClient.count("chouette.dispatched.logs.number", logs_num)
            ChouetteClient.count("chouette.dispatched.logs.bytes", message_size)
        return True
