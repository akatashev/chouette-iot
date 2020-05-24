"""
chouette.metrics.plugins.HostStatsCollector
"""
import logging
from collections import namedtuple
from itertools import chain
from typing import Iterator

import psutil

from chouette._singleton_actor import SingletonActor
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse

__all__ = ["HostStatsCollector"]

logger = logging.getLogger("chouette")


class HostStatsCollector(SingletonActor):
    """
    Actor that collects host data like RAM, CPU and HDD usage.

    NB: Collectors MUST interact with plugins via `tell` pattern.
        `ask` pattern will return None.
    """

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
            collection_methods = [
                HostCollectorPlugin.get_cpu_percentage,
                HostCollectorPlugin.get_fs_metrics,
                HostCollectorPlugin.get_ram_metrics,
            ]
            metrics = map(lambda func: func(), collection_methods)
            stats = chain.from_iterable(metrics)
            if hasattr(message.sender, "tell"):
                message.sender.tell(StatsResponse(self.name, stats))


class HostCollectorPlugin(CollectorPlugin):
    """
    CollectorPlugin that handles CPU, RAM and HDD metrics.

    Built around psutil package: https://psutil.readthedocs.io/en/latest/
    """

    @classmethod
    def get_cpu_percentage(cls) -> Iterator:
        """
        Gets CPU percentage stats via 'cpu_percent()' method:
        https://psutil.readthedocs.io/en/latest/#psutil.cpu_percent

        Documentation says that it can return a dummy 0.0 value on
        the first run, so this value is filtered from the output.

        Returns: Iterator over WrappedMetric objects.
        """
        cpu_percentage = psutil.cpu_percent()
        if cpu_percentage != 0.0:
            collecting_metrics = [("host.cpu.percentage", cpu_percentage)]
        else:
            collecting_metrics = []
        return cls._wrap_metrics(collecting_metrics)

    @classmethod
    def get_fs_metrics(cls) -> Iterator:
        """
        Gets disks usage stats.

        Collects the list of partitions and passes it to the
        `_process_filesystem` method to get actual metrics.

        See:
        https://psutil.readthedocs.io/en/latest/#psutil.disk_partitions

        Sometimes Docker returns the same partition as being mounted
        to few different mountpoints. This situation is not handled
        here.

        Returns: Iterator over WrappedMetric objects.
        """
        filesystems = psutil.disk_partitions()
        mapped = map(cls._process_filesystem, filesystems)
        metrics = chain.from_iterable(mapped)
        return metrics

    @classmethod
    def _process_filesystem(cls, filesystem: namedtuple) -> Iterator:
        """
        Gets specific filesystem disk usage stats.

        Uses `disk_usage` method to get information about used
        and free storage on a specified filesystem.
        Using this data it's possible to calculate total filesystem
        size or used space percentage in a DataDog dashboard itself.

        See:
        https://psutil.readthedocs.io/en/latest/#psutil.disk_usage

        Args:
            filesystem: psutil._common.sdiskpart object.
        Returns: Iterator over WrappedMetric objects.
        """
        tags = [f"device:{filesystem.device}"]
        fs_usage = psutil.disk_usage(filesystem.mountpoint)
        collecting_metrics = [
            ("host.fs.used", fs_usage.used),
            ("host.fs.free", fs_usage.free),
        ]
        return cls._wrap_metrics(collecting_metrics, tags=tags)

    @classmethod
    def get_ram_metrics(cls) -> Iterator:
        """
        Gets memory usage stats via `virtual_memory` method.

        Wraps data about used and available physical memory. Using
        this data it's possible to calculate total memory amount and
        memory usage percentage in a DataDog dashboard itself.

        See:
        https://psutil.readthedocs.io/en/latest/#psutil.virtual_memory

        Returns: Iterator over WrappedMetric objects.
        """
        memory = psutil.virtual_memory()
        collecting_metrics = [
            ("host.memory.used", memory.used),
            ("host.memory.available", memory.available),
        ]
        return cls._wrap_metrics(collecting_metrics)
