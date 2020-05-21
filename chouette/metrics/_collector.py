from pykka.gevent import GeventActor

from chouette import get_redis_handler
from chouette._messages import StoreMetrics
from chouette.metrics.plugins import get_host_collector


class MetricsCollector(GeventActor):
    def on_receive(self, message):
        collected_metrics = []
        plugins = [get_host_collector()]
        for plugin in plugins:
            collected_metrics.extend(plugin.ask("collect"))
        if collected_metrics:
            get_redis_handler().tell(StoreMetrics(collected_metrics))
