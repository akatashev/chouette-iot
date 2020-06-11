"""
chouette.metrics.plugins.HostStatsCollector
"""
# pylint: disable=too-few-public-methods
import logging
import time
from itertools import chain
from typing import Iterator, List

import psutil  # type: ignore
from pydantic import BaseSettings  # type: ignore
from pykka import ActorDeadError  # type: ignore

from chouette_iot._singleton_actor import SingletonActor
from chouette_iot.metrics import WrappedMetric
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse

__all__ = ["HostStatsCollector"]

logger = logging.getLogger("chouette-iot")


class HostCollectorConfig(BaseSettings):
    """
    Optional Environment variables based configuration.

    It specifies what metrics this plugin should collect. By default all
    metrics are listed here, but it's possible to request a subset of them
    via an environment variable.
    """

    # `network` stats also can be collected.
    host_collector_metrics: List[str] = ["cpu", "fs", "la", "ram"]


class HostStatsCollector(SingletonActor):
    """
    Actor that collects host stats like RAM, CPU and HDD usage.

    NB: Collectors MUST interact with plugins via `tell` pattern.
        `ask` pattern will return None.
    """

    def __init__(self):
        super().__init__()

        host_methods = {
            "cpu": HostCollectorPlugin.get_cpu_percentage,
            "fs": HostCollectorPlugin.get_fs_metrics,
            "la": HostCollectorPlugin.get_la_metrics,
            "ram": HostCollectorPlugin.get_ram_metrics,
            "network": HostCollectorPlugin.get_network_metrics,
        }

        metrics_to_send = HostCollectorConfig().host_collector_metrics
        collection_methods = [
            host_methods.get(method.lower()) for method in metrics_to_send
        ]
        self.methods = [method for method in collection_methods if method]

    def on_receive(self, message: StatsRequest) -> None:
        """
        On StatsRequest message collects specified metrics and
        sends them back in a StatsResponse message.

        On any other message does nothing.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest):
            metrics = map(lambda func: func(), self.methods)
            stats = chain.from_iterable(metrics)
            if hasattr(message.sender, "tell"):
                try:
                    message.sender.tell(StatsResponse(self.name, stats))
                except ActorDeadError:
                    logger.warning(
                        "[%s] Requester is stopped. Dropping message.", self.name
                    )


class HostCollectorPlugin(CollectorPlugin):
    """
    CollectorPlugin that handles CPU, RAM and HDD metrics.

    Built around psutil package: https://psutil.readthedocs.io/en/latest/
    """

    @classmethod
    def get_la_metrics(cls) -> Iterator[WrappedMetric]:
        """
        Gets LA stats via 'getloadavg()' method:
        https://psutil.readthedocs.io/en/latest/#psutil.getloadavg

        Returns a metric "Chouette.host.la" for 1m LA value.
        There is no real reason to send 5m and 15m metrics, but it's possible,
        commented lines should be uncommented for this.

        Returns: Iterator over WrappedMetric objects.
        """
        min_1, min_5, min_15 = psutil.getloadavg()
        m1m = cls._wrap_metrics([("Chouette.host.la", min_1)], tags=["period:1m"])
        # m5m = cls._wrap_metrics([("Chouette.host.la", min_5)], tags=["period:5m"])
        # m15m = cls._wrap_metrics([("Chouette.host.la", min_15)], tags=["period:15m"])
        # return chain(m1m, m5m, m15m)
        return m1m

    @classmethod
    def get_cpu_percentage(cls) -> Iterator[WrappedMetric]:
        """
        Gets CPU percentage stats via 'cpu_percent()' method:
        https://psutil.readthedocs.io/en/latest/#psutil.cpu_percent

        Documentation says that it can return a dummy 0.0 value on
        the first run, so this value is filtered from the output.

        Returns: Iterator over WrappedMetric objects.
        """
        cpu_percentage = psutil.cpu_percent()
        if cpu_percentage != 0.0:
            collecting_metrics = [("Chouette.host.cpu.percentage", cpu_percentage)]
        else:
            collecting_metrics = []
        return cls._wrap_metrics(collecting_metrics)

    @classmethod
    def get_fs_metrics(cls) -> Iterator[WrappedMetric]:
        """
        Gets disks usage stats.

        Collects the list of partitions and passes it to the
        `_process_filesystem` method to get actual metrics.

        See:
        https://psutil.readthedocs.io/en/latest/#psutil.disk_partitions

        Sometimes Docker returns the same partition as being mounted
        to few different mountpoints. To avoid duplicated metrics,
        WrappedMetrics have __hash__ method and set is being used to
        remove duplicates and send a correct number of metrics for every
        device.

        Returns: Iterator over WrappedMetric objects.
        """
        filesystems = psutil.disk_partitions()
        timestamp = time.time()
        mapped = [list(cls._process_filesystem(fs, timestamp)) for fs in filesystems]
        # Duplication removing hack:
        metrics = iter(set(sum(mapped, [])))
        return metrics

    @classmethod
    def _process_filesystem(cls, filesystem, timestamp) -> Iterator[WrappedMetric]:
        """
        Gets specific filesystem disk usage stats.

        Uses `disk_usage` method to get information about used
        and free storage on a specified filesystem.
        Using this data it's possible to calculate total filesystem
        size or used space percentage in a Datadog dashboard itself.

        See:
        https://psutil.readthedocs.io/en/latest/#psutil.disk_usage

        Args:
            filesystem: psutil._common.sdiskpart object.
        Returns: Iterator over WrappedMetric objects.
        """
        tags = [f"device:{filesystem.device}"]
        fs_usage = psutil.disk_usage(filesystem.mountpoint)
        collecting_metrics = [
            ("Chouette.host.fs.used", fs_usage.used),
            ("Chouette.host.fs.free", fs_usage.free),
        ]
        metrics = cls._wrap_metrics(collecting_metrics, tags=tags, timestamp=timestamp)
        return metrics

    @classmethod
    def get_ram_metrics(cls) -> Iterator[WrappedMetric]:
        """
        Gets memory usage stats via `virtual_memory` method.

        Wraps data about used and available physical memory. Using
        this data it's possible to calculate total memory amount and
        memory usage percentage in a Datadog dashboard itself.

        See:
        https://psutil.readthedocs.io/en/latest/#psutil.virtual_memory

        Returns: Iterator over WrappedMetric objects.
        """
        memory = psutil.virtual_memory()
        collecting_metrics = [
            ("Chouette.host.memory.used", memory.used),
            ("Chouette.host.memory.available", memory.available),
        ]
        return cls._wrap_metrics(collecting_metrics)

    @classmethod
    def get_network_metrics(cls) -> Iterator[WrappedMetric]:
        """
        Gets amount of sent and received bytes for all the interfaces but lo.

        Returns: Iterator over WrappedMetric objects.
        """
        interfaces_data = psutil.net_io_counters(pernic=True)
        metrics = [
            cls._process_iface(iface, data)
            for iface, data in interfaces_data.items()
            if iface != "lo"
        ]
        return chain.from_iterable(metrics)

    @classmethod
    def _process_iface(cls, iface: str, data) -> Iterator[WrappedMetric]:
        """
        Generates `bytes.sent` and `bytes.recv` metrics for a specified iface.

        Args:
            iface: Name of an interface for a tag.
            data: `snetio` namedtuple with data about networking.
        Returns: Iterator over WrappedMetric objects.
        """
        tags = [f"iface:{iface}"]
        collecting_metrics = [
            ("Chouette.host.network.bytes.sent", data.bytes_sent),
            ("Chouette.host.network.bytes.recv", data.bytes_recv),
        ]
        return cls._wrap_metrics(collecting_metrics, tags=tags)
