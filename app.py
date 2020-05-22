import logging
import time

import gevent.monkey
from pykka import ActorRegistry

from chouette import Scheduler
from chouette.metrics import MetricsCollector, MetricsAggregator

VERSION = "0.0.1a"

logger = logging.getLogger("chouette")

if __name__ == "__main__":
    logger.setLevel("INFO")
    gevent.monkey.patch_all()
    logger.critical("Starting Chouette version %s.", VERSION)
    collector = MetricsCollector.start()
    aggregator = MetricsAggregator.start()
    metrics_collection = Scheduler.schedule_at_fixed_rate(0, 10, collector.tell, "collect")
    aggregation = Scheduler.schedule_at_fixed_rate(0, 15, aggregator.tell, "aggregate")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        metrics_collection.cancel()
        aggregation.cancel()
        ActorRegistry.stop_all()
