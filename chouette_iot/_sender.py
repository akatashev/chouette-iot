"""
Sender Actor Abstract Class
"""
import logging
from typing import Any, List, Optional

import requests
from requests.exceptions import RequestException

from chouette_iot import ChouetteConfig
from chouette_iot._singleton_actor import VitalActor
from chouette_iot.storages import RedisStorage
from chouette_iot.storages.messages import (
    CollectKeys,
    CollectValues,
)

__all__ = ["Sender"]

logger = logging.getLogger("chouette-iot")


class Sender(VitalActor):
    """
    Sender is an actor that communicates to Datadog.

    That's an abstract class, actual actors must implement all
    the nonimplemented methods.
    """

    def __init__(self):
        """
        Next configuration is being extracted from ChouetteConfig:

        * api_key: Datadog API key.
        * datadog_url: Datadog URL. It has a default value.
        * log_ttl: Datadog drops outdated logs, so we clean them before
            sending data. This option says how many seconds is considered
            being "outdated". Logs older than TTL are being dropped.
        * tags: List of global tags to add to every metric. Should have
            something that gives you a chance to understand what device
            send this metrics. E.g.: a 'host' tag.
        * timeout: Maximum HTTPS Request Timeout for a metrics dispatch
            request.
        """
        super().__init__()
        config = ChouetteConfig()
        self.api_key = config.api_key
        self.bulk_size = 500  # Just to calm down the typing system.
        self.config = config
        self.host = config.host
        self.datadog_url = config.datadog_url
        self.redis = RedisStorage.get_instance()
        self.send_self_metrics = config.send_self_metrics
        self.tags = config.global_tags
        self.timeout = int(config.release_interval * 0.8)

    def on_receive(self, message: Any) -> bool:
        """
        Message handler. It's mainly the same, but should be implemented
        individually.
        """
        raise NotImplemented("Use concrete Sender implementation.")  # pragma: no cover

    def add_global_tags(self, b_record: bytes) -> Optional[dict]:
        """
        Tags should be added for most of the records, but in a slightly
        different way, so this method must be implemented individually.
        """
        raise NotImplemented("Use concrete Sender implementation.")  # pragma: no cover

    def collect_keys(self, records_type: str) -> List[bytes]:
        """
        Requests a `self.bulk_size` amount of records keys from Redis.

        It returns the oldest keys to retrieve as much data as possible in case
        of a networking outage.

        Args:
            records_type: Type of records (logs, metrics, etc).
        Returns: List of record keys as bytes.
        """
        request = CollectKeys(records_type, amount=self.bulk_size, wrapped=True)
        keys_and_ts = self.redis.ask(request)
        logger.debug("[%s] Collected %s %s.", self.name, len(keys_and_ts), records_type)
        return list(map(lambda pair: pair[0], keys_and_ts))

    def collect_records(self, keys: List[bytes], records_type: str) -> List[dict]:
        """
        Gets a list of records from Redis, adds global tags to them and prepare
        them to be dispatched to Datadog.

        Args:
            keys: List of records keys as bytes.
            records_type: Type of records (logs, metrics, etc).
        Returns: List of prepared to dispatch objects.
        """
        b_records = self.redis.ask(CollectValues(records_type, keys, wrapped=True))
        logger.debug("[%s] Collected %s %s.", self.name, len(b_records), records_type)
        return list(filter(None, map(self.add_global_tags, b_records)))

    def dispatch_to_datadog(self, metrics: List[dict]) -> bool:
        """
        Datadog dispatching logic must be implemented individually.
        """
        raise NotImplemented("Use concrete Sender implementation.")  # pragma: no cover

    def _post_to_datadog(self, message: bytes, dd_endpoint: str) -> bool:
        """
        Implements actual HTTPS interaction with Datadog.

        On message 202 Accepted returns True, on any other message or
        RequestsException returns False and logs an error message.

        Arg:
            message: Compressed message to sent.
            dd_endpoint: Datadog endpoint where we should send a message.
        Return: Bool that shows whether the message was accepted.
        """
        try:
            dd_response = requests.post(
                f"{self.datadog_url}/{dd_endpoint}",
                params={"api_key": self.api_key},
                data=message,
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "deflate",
                },
                timeout=self.timeout,
            )
            if not dd_response.status_code in [200, 202]:
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
