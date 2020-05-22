from pykka import ActorRegistry

from ._host_collector import HostStatsCollector

__all__ = ["HostStatsCollector", "get_host_collector"]


def get_host_collector():
    host_collectors = ActorRegistry.get_by_class(HostStatsCollector)
    if host_collectors:
        return host_collectors.pop()
    else:
        return HostStatsCollector.start()
