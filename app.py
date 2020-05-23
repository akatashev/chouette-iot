import logging
import time

import gevent.monkey
from pykka import ActorRegistry

from chouette import Scheduler, ChouetteConfig
from chouette.metrics import MetricsCollector, MetricsAggregator, MetricsSender

VERSION = "0.0.1a"

logger = logging.getLogger("chouette")

if __name__ == "__main__":
    logger.setLevel("INFO")
    gevent.monkey.patch_all()
    logger.critical("Starting Chouette version %s.", VERSION)
    config = ChouetteConfig()
    collector = MetricsCollector.start()
    aggregator = MetricsAggregator.start()
    sender = MetricsSender.start()
    metrics_collection = Scheduler.schedule_at_fixed_rate(0, config.capture_interval, collector.tell, "collect")
    metrics_aggregation = Scheduler.schedule_at_fixed_rate(0, config.aggregate_interval, aggregator.tell, "aggregate")
    metrics_release = Scheduler.schedule_at_fixed_rate(0, config.release_interval, sender.tell, "send")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        metrics_collection.cancel()
        metrics_aggregation.cancel()
        metrics_release.cancel()
        ActorRegistry.stop_all()
