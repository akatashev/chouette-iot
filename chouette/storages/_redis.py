"""
Actor that handles all interactions with the Redis storage.
"""
import json
import logging
import time
from typing import Any, List, Tuple, Union
from uuid import uuid4

from redis import Redis, RedisError

from chouette._singleton_actor import SingletonActor
from chouette.storages.messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreRecords,
)
from pydantic import BaseSettings

__all__ = ["RedisStorage"]

logger = logging.getLogger("chouette")


class RedisConfig(BaseSettings):
    """
    RedisStorage environment configuration object.
    Reads Redis' host and port from environment variables if called.
    """

    redis_host: str = "redis"
    redis_port: int = 6379


class RedisStorage(SingletonActor):
    """
    Redis handling Singleton Actor.

    Handles requests to collect, store, and delete different kinds of records
    from the Redis storage.

    Records themselves are stored in Redis hashes like this: {key: metric body}
    Their keys are being stored in Sorted Sets sorted by timestamps.

    Records can be `raw` (unprocessed, received from clients) or `wrapped`
    (processed, prepared to dispatch). They are being stored in different queues.

    The queue for raw metrics is named like this:
    Sorted set: `chouette:raw:metrics.keys`.
    Hash: `chouette:raw:metrics.values`.

    This pattern `chouette:(raw/wrapped):(record type).(keys/values)` is used
    for all the queues.

    Is intentionally expected to be used with the `ask` pattern to ensure that
    consumers always execute their logic in a correct order.
    """

    def __init__(self):
        """
        RedisStorage uses RedisConfig to create a Redis object.
        """
        super().__init__()
        config = RedisConfig()
        self.redis = Redis(host=config.redis_host, port=config.redis_port)

    def on_receive(self, message: Any) -> Union[list, bool]:
        """
        Messages handling routine.

        It takes any message, but it will actively process only valid
        storage messages from the `chouette.storages.messages` package.

        Args:
            message: Anything. Expected to be a valid storage message.
        Returns: Either a List of bytes or a bool.
        """
        if isinstance(message, CleanupOutdatedRecords):
            return self._cleanup_outdated(message)

        if isinstance(message, CollectKeys):
            return self._collect_keys(message)

        if isinstance(message, CollectValues):
            return self._collect_values(message)

        if isinstance(message, DeleteRecords):
            return self._delete_records(message)

        if isinstance(message, StoreRecords):
            return self._store_records(message)

    def _cleanup_outdated(self, request: CleanupOutdatedRecords) -> bool:
        """
        Cleans up outdated records in a specified queue.

        DataDog rejects metrics older than 4 hours (default TTL), so before
        trying to dispatch anything Chouette cleans up outdated metrics.

        Args:
            request: CleanupOutdated message with record type and TTL.
        Returns: True if there were no Redis error during the task execution.
        """
        set_name = f"chouette:wrapped:{request.data_type}.keys"
        hash_name = f"chouette:wrapped:{request.data_type}.values"
        threshold = time.time() - request.ttl
        try:
            outdated_keys = self.redis.zrangebyscore(set_name, 0, threshold)
        except RedisError:
            return False
        pipeline = self.redis.pipeline()
        pipeline.zremrangebyscore(set_name, 0, threshold)
        pipeline.hdel(hash_name, *outdated_keys)
        try:
            pipeline.execute()
        except RedisError:
            return False
        return True

    def _collect_keys(self, request: CollectKeys) -> List[Tuple[int, bytes]]:
        set_type = "wrapped" if request.wrapped else "raw"
        set_name = f"chouette:{set_type}:{request.data_type}.keys"
        try:
            keys = self.redis.zrange(set_name, 0, request.amount - 1, withscores=True)
        except RedisError:
            keys = []
        logger.debug(
            "%s: Collected %s %s records keys from Redis.",
            self.__class__.__name__,
            len(keys),
            request.data_type,
        )
        return keys

    def _collect_values(self, request: CollectValues) -> List[bytes]:
        queue_type = "wrapped" if request.wrapped else "raw"
        hash_name = f"chouette:{queue_type}:{request.data_type}.values"
        try:
            raw_values = self.redis.hmget(hash_name, *list(request.keys))
            values = list(filter(None, raw_values))
        except RedisError:
            values = []
        logger.debug(
            "%s: Collected %s %s records from Redis.",
            self.__class__.__name__,
            len(request.keys),
            request.data_type,
        )
        return values

    def _delete_records(self, request: DeleteRecords) -> bool:
        if not request.keys:
            return True
        pipeline = self.redis.pipeline()
        queue_type = "wrapped" if request.wrapped else "raw"
        set_name = f"chouette:{queue_type}:{request.data_type}.keys"
        hash_name = f"chouette:{queue_type}:{request.data_type}.values"
        pipeline.zrem(set_name, *request.keys)
        pipeline.hdel(hash_name, *request.keys)
        logger.debug(
            "%s: Removing %s %s records from Redis.",
            self.__class__.__name__,
            len(request.keys),
            request.data_type,
        )
        try:
            pipeline.execute()
        except RedisError:
            return False
        return True

    def _store_records(self, request: StoreRecords) -> bool:
        pipeline = self.redis.pipeline()
        queue_type = "wrapped" if request.wrapped else "raw"
        set_name = f"chouette:{queue_type}:{request.data_type}.keys"
        hash_name = f"chouette:{queue_type}:{request.data_type}.values"
        try:
            for record in request.records:
                record_key = str(uuid4())
                pipeline.zadd(set_name, {record_key: record.timestamp})
                pipeline.hset(hash_name, record_key, json.dumps(record.asdict()))
            pipeline.execute()
        except (AttributeError, RedisError):
            return False
        return True
