from pykka import ActorRegistry
from ._host import HostCollectingPlugin

__all__ = ["HostCollectingPlugin", "get_host_collector"]


def get_host_collector():
    host_collectors = ActorRegistry.get_by_class(HostCollectingPlugin)
    if host_collectors:
        return host_collectors.pop()
    else:
        return HostCollectingPlugin.start()
