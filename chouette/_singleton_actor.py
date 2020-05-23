"""
SingletonActor class.
"""
import logging

from pykka import ActorRegistry, ActorRef
from pykka.gevent import GeventActor

__all__ = ["SingletonActor"]

logger = logging.getLogger("chouette")


class SingletonActor(GeventActor):
    """
    SingletonActor is a wrapper around pykka actor objects.

    In Chouette workflow we normally use just one instance of an actor.
    SingletonActor is able to return an ActorRef of a running instance
    of its class or to start a new instance and return its ActorRef.
    """

    def __init__(self):
        super().__init__()
        self.name = self.__class__.__name__

    @classmethod
    def get_instance(cls) -> ActorRef:
        """
        Returns a running instance of an actor from ActorRegistry or
        starts a new instance of an actor.

        Returns: ActorRef.
        """
        instances = ActorRegistry.get_by_class(cls)
        if instances:
            return instances.pop()
        return cls.start()

    def on_failure(self, exception_type, exception_value, traceback):
        logger.error(
            "[%s] Stopped with exception: %s: %s.",
            self.name,
            exception_type,
            exception_value,
        )
        traceback.print_exc()
