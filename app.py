import logging
import sys
import time

import gevent.monkey  # type: ignore
gevent.monkey.patch_all()
from pythonjsonlogger import jsonlogger  # type: ignore

from chouette._singleton_actor import SingletonActor

from pykka import ActorRegistry  # type: ignore
from typing import List, Type

from chouette import Scheduler, ChouetteConfig, Cancellable
from chouette.metrics import MetricsCollector, MetricsAggregator, MetricsSender

logger = logging.getLogger("chouette")


class Chouette:
    @staticmethod
    def setup_logging(log_level: str) -> None:
        """
        Configures logger to use JSON format and set s the desired log level.

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
            interval, actor_class: Type[SingletonActor], message
    ) -> Cancellable:
        actor_ref = actor_class.get_instance()
        initial_delay = interval - (time.time() % interval)
        timer = Scheduler.schedule_at_fixed_rate(
            initial_delay, interval, actor_ref.tell, message
        )
        return timer

    @classmethod
    def run(cls) -> List[Cancellable]:
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
    timers = Chouette.run()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for timer in timers:
            timer.cancel()
        ActorRegistry.stop_all()
