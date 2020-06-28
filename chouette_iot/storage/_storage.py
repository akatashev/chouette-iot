"""
Actor that handles all interactions with the Redis storage.
"""
# pylint: disable=too-few-public-methods
import logging
from typing import Any, Union

from chouette_iot import ChouetteConfig
from chouette_iot._singleton_actor import SingletonActor
from .engines import EnginesFactory
from .messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    GetQueueSize,
    StoreRecords,
)

__all__ = ["StorageActor"]

logger = logging.getLogger("chouette-iot")


class StorageActor(SingletonActor):
    """
    Redis handling Singleton Actor.

    Handles requests to collect, store, and delete different kinds of records
    from the Redis storage.

    Records themselves are stored in Redis hashes like this: {key: metric body}
    Their keys are being stored in Sorted Sets sorted by timestamps.

    Records can be `raw` (unprocessed, received from clients) or `wrapped`
    (prepared to dispatch). They are being stored in different queues.

    The queue for raw metrics is named like this:
    Sorted set: `chouette:raw:metrics.keys`.
    Hash: `chouette:raw:metrics.values`.

    This pattern `chouette:(raw/wrapped):(record type).(keys/values)` is used
    for all the queues.

    Is intentionally expected to be almost always used with `ask` pattern
    to ensure that consumers always execute their logic in a correct order.
    """

    def __init__(self):
        super().__init__()
        self.storage = EnginesFactory.get_engine(ChouetteConfig().storage_type)

    def on_receive(self, message: Any) -> Union[int, list, bool, None]:
        """
        Messages handling routine.

        It takes any message, but it will actively process only valid
        storage messages from the `chouette.storage.messages` package.

        All other messages do nothing and receive None if `ask` was used
        to send them.

        Args:
            message: Anything. Expected to be a valid storage message.
        Returns: Either a List of bytes or a bool.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, CleanupOutdatedRecords):
            return self.storage.cleanup_outdated(message)

        if isinstance(message, CollectKeys):
            return self.storage.collect_keys(message)

        if isinstance(message, CollectValues):
            return self.storage.collect_values(message)

        if isinstance(message, DeleteRecords):
            return self.storage.delete_records(message)

        if isinstance(message, GetQueueSize):
            return self.storage.get_queue_size(message)

        if isinstance(message, StoreRecords):
            return self.storage.store_records(message)

        return None
