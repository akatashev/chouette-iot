"""
Storage Engine for Redis storage type.
"""
import json
import logging
import time
from typing import Any, List, Tuple
from uuid import uuid4

from pydantic import BaseSettings
from redis import Redis, RedisError

from ._storage_engine import StorageEngine
from ..messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    GetQueueSize,
    StoreRecords,
)

__all__ = ["RedisEngine"]

logger = logging.getLogger("chouette-iot")


class RedisConfig(BaseSettings):
    """
    RedisStorage environment configuration object.
    Reads Redis' host and port from environment variables if called.
    """

    redis_host: str = "redis"
    redis_port: int = 6379


class RedisEngine(StorageEngine):
    """
    Storage engine for Redis storage type.
    """

    def __init__(self):
        config = RedisConfig()
        self.redis = Redis(host=config.redis_host, port=config.redis_port)
        # Different versions of Redis use different HSET command formats:
        redis_version = self.redis.info().get("redis_version")
        self.redis_version = int(redis_version.split(".")[0])
        self.name = self.__class__.__name__

    def stop(self):
        """
        Tries to release connections to Redis if there are any.
        """
        self.redis.close()

    def cleanup_outdated(self, request: CleanupOutdatedRecords) -> bool:
        """
        Cleans up outdated records in a specified queue.

        Datadog rejects metrics older than 4 hours (default TTL), so before
        trying to dispatch anything Chouette cleans up outdated metrics.

        Args:
            request: CleanupOutdated message with record type and TTL.
        Returns: Boolean that says whether execution was successful.
        """
        queue_name, set_name, hash_name = self._get_queue_names(request)
        threshold = time.time() - request.ttl
        try:
            outdated_keys = self.redis.zrangebyscore(set_name, 0, threshold)
            if not outdated_keys:
                logger.debug(
                    "[%s] No outdated records to cleanup in a queue '%s'",
                    self.name,
                    queue_name,
                )
                return True
            pipeline = self.redis.pipeline()
            pipeline.zremrangebyscore(set_name, 0, threshold)
            pipeline.hdel(hash_name, *outdated_keys)
            pipeline.execute()
            logger.debug(
                "[%s] Cleaned %s outdated records from a queue '%s'.",
                self.name,
                len(outdated_keys),
                queue_name,
            )
        except RedisError as error:
            logger.warning(
                "[%s] Could not cleanup outdated records in a queue '%s' due to: '%s'.",
                self.name,
                queue_name,
                error,
            )
            return False
        return True

    def collect_keys(self, request: CollectKeys) -> List[Tuple[bytes, int]]:
        """
        Tries to collect keys from a specified queue.

        CollectKeys message has the following properties:
        * data_type - type of a queue, e.g.: 'metrics'.
        * wrapped - whether that's a queue of processed records or not.
        * amount - how many keys should be collected. 0 means `all of them`.

        It returns a list of tuples with keys and their timestamps:
        (key: bytes, timestamp: int).

        Args:
            request: CollectKeys message.
        Returns: List of collected keys as tuples.
        """
        queue_name, set_name, _ = self._get_queue_names(request)
        try:
            keys = self.redis.zrange(set_name, 0, request.amount - 1, withscores=True)
        except RedisError as error:
            logger.warning(
                "[%s] Could not collect keys from a queue '%s' due to: '%s'.",
                self.name,
                queue_name,
                error,
            )
            return []
        logger.debug(
            "[%s] Collected %s keys from a queue '%s'.",
            self.name,
            len(keys),
            queue_name,
        )
        return keys

    def collect_values(self, request: CollectValues) -> List[bytes]:
        """
        Tries to collect values by keys from a specified queue.

        Receives a CollectValues message that contains a list of keys.
        It goes to this queue's hash and gets corresponding records as bytes.

        Args:
            request: CollectValues message with specified keys.
        Returns: List of collected values.
        """
        queue_name, _, hash_name = self._get_queue_names(request)
        if not request.keys:
            logger.debug(
                "[%s] No keys were specified to collect values for a queue '%s'.",
                self.name,
                queue_name,
            )
            return []
        try:
            raw_values = self.redis.hmget(hash_name, *request.keys)
            values: List[bytes] = [value for value in raw_values if value]
        except RedisError as error:
            logger.warning(
                "[%s] Could not collect records from a queue '%s' due to: '%s'.",
                self.name,
                queue_name,
                error,
            )
            return []
        logger.debug(
            "[%s] Collected %s records from a queue '%s'.",
            self.name,
            len(values),
            queue_name,
        )
        return values

    def delete_records(self, request: DeleteRecords) -> bool:
        """
        Tries to delete records with specified keys.

        Args:
            request: DeleteRecords message with specified keys.
        Returns: Boolean that says whether execution was successful.
        """
        queue_name, set_name, hash_name = self._get_queue_names(request)
        if not request.keys:
            logger.debug(
                "[%s] Nothing to delete from a queue '%s'.", self.name, queue_name
            )
            return True
        pipeline = self.redis.pipeline()
        pipeline.zrem(set_name, *request.keys)
        pipeline.hdel(hash_name, *request.keys)
        try:
            pipeline.execute()
        except RedisError as error:
            logger.warning(
                "[%s] Could not remove %s records from a queue '%s' due to: '%s'.",
                self.name,
                len(request.keys),
                queue_name,
                error,
            )
            return False
        logger.debug(
            "[%s] Deleted %s records from a queue '%s'.",
            self.name,
            len(request.keys),
            queue_name,
        )
        return True

    def get_queue_size(self, request: GetQueueSize) -> int:
        """
        Tried to get a size of a specified queue.

        In case of error returns -1, because values less than 1 SHOULD be
        filtered. 0 usually means that this queue doesn't exist, so we can't
        really talk about it size.

        Args:
            request: GetQueueSize message.
        Returns: Size of a specified queue.
        """
        queue_name, _, hash_name = self._get_queue_names(request)
        try:
            queue_size = int(self.redis.hlen(hash_name))
        except RedisError as error:
            logger.warning(
                "[%s] Could not calculate %s queue size due to: '%s'.",
                self.name,
                queue_name,
                error,
            )
            return -1
        return queue_size

    def store_records(self, request: StoreRecords) -> bool:
        """
        Tries to store received records to a queue.

        It automatically generates a unique id for every record and stores
        its content under this id both to a set and a hash.

        If it can't cast one of the records to a dict via `asdict()` method,
        it ignores this record and tries to store all other records.

        Args:
            request: StoreRecords with an iterable of suitable objects.
        Returns: Boolean that says whether execution was successful.
        """
        queue_name, set_name, hash_name = self._get_queue_names(request)
        pipeline = self.redis.pipeline()
        records_list = list(request.records)
        keys = {}
        values = {}
        for record in records_list:
            try:
                record_value = json.dumps(record.asdict())
            except AttributeError:
                continue
            record_key = str(uuid4())
            keys[record_key] = record.timestamp
            values[record_key] = record_value
        stored_metrics = len(values)
        if not values:
            logger.debug(
                "[%s] Nothing to store to a queue '%s'.", self.name, queue_name
            )
            return True
        try:
            pipeline.zadd(set_name, mapping=keys)
            if self.redis_version >= 4:
                # From Redis 4.0.0 HMSET command is deprecated.
                pipeline.hset(hash_name, mapping=values)
            else:
                # Before Redis 4.0.0 HSET command took only 2 arguments:
                pipeline.hmset(hash_name, mapping=values)
            pipeline.execute()
        except (RedisError, TypeError) as error:
            logger.warning(
                "[%s] Could not store %s/%s records to a queue '%s' due to: '%s'.",
                self.name,
                stored_metrics,
                len(records_list),
                queue_name,
                error,
            )
            return False
        logger.debug(
            "[%s] Stored %s/%s records to a queue '%s'.",
            self.name,
            stored_metrics,
            len(records_list),
            queue_name,
        )
        return True

    @staticmethod
    def _get_queue_names(request: Any) -> Tuple[str, str, str]:
        """
        Generates queue, set and hash name for a queue depending on a request.

        Args:
            request: One of `chouette.storage.messages` objects.
        Return: Tuple of a queue name, a set name and a hash name as strings.
        """
        queue_type = "wrapped" if request.wrapped else "raw"
        queue_name = f"chouette:{request.data_type}:{queue_type}"
        set_name = f"{queue_name}.keys"
        hash_name = f"{queue_name}.values"
        return queue_name, set_name, hash_name
