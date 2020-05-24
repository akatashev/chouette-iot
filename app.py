import logging
import time

import gevent.monkey
from pykka import ActorRegistry

from chouette import Scheduler, ChouetteConfig
from chouette.metrics import MetricsCollector, MetricsAggregator, MetricsSender


logger = logging.getLogger("chouette")

if __name__ == "__main__":
    """
    TODO: REFACTOR, REFACTOR, REFACTOR.
    """
    gevent.monkey.patch_all()
    config = ChouetteConfig()
    logger.setLevel(config.log_level)
    collector = MetricsCollector.start()
    aggregator = MetricsAggregator.start()
    sender = MetricsSender.start()
    timers = []
    if config.collector_plugins:
        collector = MetricsCollector.start()
        timers.append(Scheduler.schedule_at_fixed_rate(0, config.capture_interval, collector.tell, "collect"))
    timers.append(Scheduler.schedule_at_fixed_rate(0, config.aggregate_interval, aggregator.tell, "aggregate"))
    timers.append(Scheduler.schedule_at_fixed_rate(0, config.release_interval, sender.tell, "send"))
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for timer in timers:
            timer.cancel()
        ActorRegistry.stop_all()
