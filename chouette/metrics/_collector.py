from pykka.gevent import GeventActor
from functools import reduce
from itertools import chain
from chouette import get_redis_handler
from chouette.messages import StoreMetrics
from chouette.metrics.plugins import get_host_collector


class MetricsCollector(GeventActor):
    def on_receive(self, message):
        plugins = [get_host_collector()]
        mapped_metrics = map(lambda plugin: plugin.ask("collect"), plugins)
        collected_metrics = reduce(lambda a, b: chain(a, b), mapped_metrics)
        if collected_metrics:
            get_redis_handler().tell(StoreMetrics(collected_metrics))
