import logging
import sys
import time

import gevent.monkey
from pythonjsonlogger import jsonlogger

gevent.monkey.patch_all()
from pykka import ActorRegistry

from chouette import Scheduler, ChouetteConfig
from chouette.metrics import MetricsCollector, MetricsAggregator, MetricsSender

logger = logging.getLogger("chouette")


def setup_logging(log_level):
    """
    Configures logger to use JSON format and support dramatiq messages,
    sets the desired log level.
    :return: None
    """
    #: Setup logging
    stdout_handler = logging.StreamHandler(sys.stdout)

    json_formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z"
    )

    stdout_handler.setFormatter(json_formatter)

    # Setup the Basic application logger
    logging.basicConfig(level=log_level, handlers=[stdout_handler])


if __name__ == "__main__":
    """
    TODO: REFACTOR, REFACTOR, REFACTOR.
    """
    config = ChouetteConfig()
    setup_logging(config.log_level)
    collector = MetricsCollector.start()
    aggregator = MetricsAggregator.start()
    sender = MetricsSender.start()
    timers = []
    initial_delay = 60 - (time.time() % 60)
    if config.collector_plugins:
        collector = MetricsCollector.start()
        timers.append(
            Scheduler.schedule_at_fixed_rate(
                initial_delay, config.capture_interval, collector.tell, "collect"
            )
        )
    timers.append(
        Scheduler.schedule_at_fixed_rate(
            initial_delay, config.aggregate_interval, aggregator.tell, "aggregate"
        )
    )
    timers.append(
        Scheduler.schedule_at_fixed_rate(
            initial_delay, config.release_interval, sender.tell, "send"
        )
    )
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for timer in timers:
            timer.cancel()
        ActorRegistry.stop_all()
