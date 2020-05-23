import json
import logging
import time
from typing import List
from uuid import uuid4

from redis import Redis, RedisError

from chouette import ChouetteConfig
from chouette._singleton_actor import SingletonActor
from chouette.storages.messages import (
    CleanupOutdated,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    StoreMetrics,
)

logger = logging.getLogger("chouette")


class RedisHandler(SingletonActor):
    def __init__(self):
        super().__init__()
        config = ChouetteConfig()
        self.redis = Redis(host=config.redis_host, port=config.redis_port)

    def on_receive(self, message):
        if isinstance(message, CleanupOutdated):
            return self._cleanup_outdated(message)

        if isinstance(message, CollectKeys):
            return self._collect_keys(message)

        if isinstance(message, CollectValues):
            return self._collect_values(message)

        if isinstance(message, DeleteRecords):
            return self._delete_records(message)

        if isinstance(message, StoreMetrics):
            return self._store_wrapped_metrics(message)

    def _cleanup_outdated(self, request: CleanupOutdated) -> bool:
        set_name = f"chouette:wrapped:{request.data_type}.keys"
        hash_name = f"chouette:wrapped:{request.data_type}.values"
        threshold = time.time() - request.metric_ttl
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

    def _collect_keys(self, request: CollectKeys) -> List[bytes]:
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
            values = filter(None, raw_values)
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

    def _store_wrapped_metrics(self, request: StoreMetrics) -> bool:
        set_name = f"chouette:wrapped:metrics.keys"
        hash_name = f"chouette:wrapped:metrics.values"
        pipeline = self.redis.pipeline()
        for record in request.records:
            record_key = str(uuid4())
            pipeline.zadd(set_name, {record_key: record.timestamp})
            pipeline.hset(hash_name, record_key, json.dumps(record.asdict()))
        try:
            pipeline.execute()
        except RedisError:
            return False
        return True
