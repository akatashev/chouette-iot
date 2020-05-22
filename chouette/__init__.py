from chouette._scheduler import Cancellable, Scheduler
from chouette._configuration import ChouetteConfig
from pykka import ActorRegistry
from chouette._redis_handler import RedisHandler

__all__ = ["Cancellable", "Scheduler", "ChouetteConfig", "get_redis_handler"]


def get_redis_handler():
    redis_handlers = ActorRegistry.get_by_class_name("RedisHandler")
    if redis_handlers:
        return redis_handlers.pop()
    return RedisHandler.start()
