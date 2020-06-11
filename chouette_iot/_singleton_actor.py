"""
SingletonActor class.
"""
import logging

from pykka import ActorRef, ActorRegistry, ThreadingActor  # type: ignore
from chouette_iot import Scheduler

__all__ = ["SingletonActor", "VitalActor"]

logger = logging.getLogger("chouette-iot")


class SingletonActor(ThreadingActor):
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

    def on_failure(
        self, exception_type: str, exception_value: str, traceback
    ) -> None:  # pragma: no cover
        """
        Logs an exception if the actor is crashed.

        Args:
            exception_type: Exception type as a string.
            exception_value: Exception value as a string.
            traceback: Traceback object.
        Returns: None.
        """
        logger.error(
            "[%s] Stopped with exception: %s: %s.",
            self.name,
            exception_type,
            exception_value,
            exc_info=True,
        )


class VitalActor(SingletonActor):
    """
    This class represents an actor, that can't be simply restarted.
    If it stopped, the application is stopped with a critical error.
    """

    def on_failure(self, exception_type: str, exception_value: str, traceback) -> None:
        """
        Stops all the actors and all the running timers.
        """
        super().on_failure(exception_type, exception_value, traceback)
        logger.critical("[%s] Stopping Chouette.", self.name)
        ActorRegistry.stop_all()
        Scheduler.stop_all()
