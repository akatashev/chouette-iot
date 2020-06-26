"""
Actor that handles all interactions with the Redis storage.
"""
# pylint: disable=too-few-public-methods
import json
import logging
import time
from typing import Any, List, Tuple, Union
from uuid import uuid4

from pydantic import BaseSettings  # type: ignore
from redis import Redis, RedisError

from chouette_iot._singleton_actor import SingletonActor
from ._redis_messages import GetRedisQueues, GetHashSizes
from .messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreRecords,
)

__all__ = ["RedisStorage"]

logger = logging.getLogger("chouette-iot")


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
        """
        RedisStorage uses RedisConfig to create a Redis object.
        """
        super().__init__()
        config = RedisConfig()
        self.redis = Redis(host=config.redis_host, port=config.redis_port)
        # Different versions of Redis use different HSET command formats:
        redis_version = self.redis.info().get("redis_version")
        self.redis_version = int(redis_version.split(".")[0])

    def on_receive(self, message: Any) -> Union[list, bool, None]:
        """
        Messages handling routine.

        It takes any message, but it will actively process only valid
        storage messages from the `chouette.storages.messages` package.

        All other messages do nothing and receive None if `ask` was used
        to send them.

        Args:
            message: Anything. Expected to be a valid storage message.
        Returns: Either a List of bytes or a bool.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, CleanupOutdatedRecords):
            return self._cleanup_outdated(message)

        if isinstance(message, CollectKeys):
            return self._collect_keys(message)

        if isinstance(message, CollectValues):
            return self._collect_values(message)

        if isinstance(message, DeleteRecords):
            return self._delete_records(message)

        if isinstance(message, GetHashSizes):
            return self._get_hash_sizes(message)

        if isinstance(message, GetRedisQueues):
            return self._get_redis_queues(message)

        if isinstance(message, StoreRecords):
            return self._store_records(message)

        return None

    def _cleanup_outdated(self, request: CleanupOutdatedRecords) -> bool:
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
                "[%s] Could not cleanup records in a queue '%s' due to: '%s'.",
                self.name,
                queue_name,
                error,
            )
            return False
        return True

    def _collect_keys(self, request: CollectKeys) -> List[Tuple[bytes, int]]:
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

    def _collect_values(self, request: CollectValues) -> List[bytes]:
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
            values: List[bytes] = list(filter(None, raw_values))
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

    def _delete_records(self, request: DeleteRecords) -> bool:
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

    def _get_hash_sizes(self, request: GetHashSizes) -> List[Tuple[str, int]]:
        """
        Returns a list of tuples with hashes names and sizes.

        Args:
            request: GetHashSizes message with a list of hashes.
        Return: List of tuples with hashes names and sizes.
        """
        try:
            hash_sizes = [
                (
                    hash_name.decode() if isinstance(hash_name, bytes) else hash_name,
                    int(self.redis.hlen(hash_name)),
                )
                for hash_name in request.hashes
            ]
        except RedisError as error:
            logger.warning(
                "[%s] Could not calculate hash sizes due to: '%s'.", self.name, error
            )
            return []
        return hash_sizes

    def _get_redis_queues(self, request: GetRedisQueues) -> List[bytes]:
        """
        Gets a list of Redis Keys (sets, hashes, lists, etc) using a specified
        key name pattern.

        Args:
            request: GetRedisKeys message with a specified pattern.
        Returns: List of found Redis Keys.
        """
        try:
            redis_keys = self.redis.keys(request.pattern)
        except RedisError as error:
            logger.warning(
                "[%s] Could not collect Redis keys for a pattern %s due to: '%s'.",
                self.name,
                request.pattern,
                error,
            )
            return []
        return redis_keys

    def _store_records(self, request: StoreRecords) -> bool:
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
                "[%s] Could not store %s/%s records to queue '%s' due to: '%s'.",
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
