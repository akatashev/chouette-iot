"""
Chouette __main__ entry point.
"""
import logging
import sys
import time
from typing import Any, List, Type

from pythonjsonlogger import jsonlogger  # type: ignore

from chouette_iot import Scheduler, ChouetteConfig, Cancellable
from chouette_iot._singleton_actor import SingletonActor
from chouette_iot.metrics import MetricsCollector, MetricsAggregator, MetricsSender

logger = logging.getLogger("chouette-iot")


class Chouette:
    """
    Chouette entry point class that creates vital actors and their timers.
    """

    @staticmethod
    def setup_logging(log_level: str) -> None:
        """
        Configures logger to use JSON format and set s the desired log level.

        Args:
            log_level: Desired log level.
        Returns: None
        """
        stdout_handler = logging.StreamHandler(sys.stdout)
        json_formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z"
        )
        stdout_handler.setFormatter(json_formatter)
        logging.basicConfig(level=log_level, handlers=[stdout_handler])

    @staticmethod
    def schedule_call(
        interval: float, actor_class: Type[SingletonActor], message: Any
    ) -> Cancellable:
        """
        Uses chouette-iot.Scheduler to periodically send a message to an actor of
        a specified class at some fixed rate.

        Args:
            interval: How often a message must be sent to an actor.
            actor_class: Class of a SingleActor child class to start.
            message: Message to send.
        Returns: Cancellable object.
        """
        actor_ref = actor_class.get_instance()
        initial_delay = interval - (time.time() % interval)
        timer = Scheduler.schedule_at_fixed_rate(
            initial_delay, interval, actor_ref.tell, message
        )
        return timer

    @classmethod
    def run(cls) -> List[Cancellable]:
        """
        Reads configuration from environment variables and creates timers to
        send messages to created actors.

        It starts a Sender actor and an Aggregator actor.
        If COLLECTOR_PLUGINS environment variable is set, it also starts a
        Collector plugin.

        Returns: List of Cancellables.
        """
        timers = []
        config = ChouetteConfig()
        cls.setup_logging(config.log_level)
        # Sender actor:
        timers.append(cls.schedule_call(config.release_interval, MetricsSender, "send"))
        # Aggregator actor:
        timers.append(
            cls.schedule_call(config.aggregate_interval, MetricsAggregator, "aggregate")
        )
        # Collector actor:
        if not config.collector_plugins:
            return timers
        timers.append(
            cls.schedule_call(config.capture_interval, MetricsCollector, "collect")
        )
        return timers


if __name__ == "__main__":
    logger.info("Starting Chouette-Iot.")
    Chouette.run()
