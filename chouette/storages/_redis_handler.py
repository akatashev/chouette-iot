import json
import logging
from uuid import uuid4
from pykka import ActorRegistry
from pykka.gevent import GeventActor
from redis import Redis, RedisError

from chouette import ChouetteConfig
from chouette.messages import CollectKeys, CollectValues, DeleteRecords, StoreMetrics

logger = logging.getLogger("chouette")


class RedisHandler(GeventActor):
    def __init__(self):
        super().__init__()
        config = ChouetteConfig()
        self.redis_client = Redis(host=config.redis_host, port=config.redis_port)

    @classmethod
    def get_instance(cls):
        instances = ActorRegistry.get_by_class(cls)
        if instances:
            return instances.pop()
        return cls.start()

    def on_receive(self, message):
        if isinstance(message, CollectKeys):
            return self.collect_keys(message)

        if isinstance(message, CollectValues):
            return self.collect_values(message)

        if isinstance(message, DeleteRecords):
            return self.delete_records(message)

        if isinstance(message, StoreMetrics):
            return self.store_wrapped_metrics(message)

    def collect_keys(self, request) -> list:
        set_type = "wrapped" if request.wrapped else "raw"
        set_name = f"chouette:{set_type}:{request.data_type}.keys"
        try:
            keys = self.redis_client.zrange(set_name, 0, -1, withscores=True)
        except RedisError:
            keys = []
        logger.debug(
            "%s: Collected %s %s records keys from Redis.",
            self.__class__.__name__,
            len(keys),
            request.data_type,
        )
        return keys

    def collect_values(self, request) -> list:
        queue_type = "wrapped" if request.wrapped else "raw"
        hash_name = f"chouette:{queue_type}:{request.data_type}.values"
        try:
            raw_values = self.redis_client.hmget(hash_name, *list(request.keys))
            values = filter(None, raw_values)
        except RedisError:
            # Todo: Fix error message
            logger.error("Aggregation error.")
            values = []
        logger.debug(
            "%s: Collected %s %s records from Redis.",
            self.__class__.__name__,
            len(request.keys),
            request.data_type,
        )
        return values

    def delete_records(self, request) -> bool:
        pipeline = self.redis_client.pipeline()
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

    def store_wrapped_metrics(self, request) -> bool:
        set_name = f"chouette:wrapped:metrics.keys"
        hash_name = f"chouette:wrapped:metrics.values"
        pipeline = self.redis_client.pipeline()
        for record in request.records:
            record_key = str(uuid4())
            pipeline.zadd(set_name, {record_key: record.timestamp})
            pipeline.hset(hash_name, record_key, json.dumps(record.asdict()))
        try:
            pipeline.execute()
        except RedisError:
            return False
        return True
