import time
from collections import namedtuple
from functools import reduce
from itertools import chain
from typing import Any, Iterator

import psutil
from pykka.gevent import GeventActor

from ._collector_plugin import CollectorPlugin


class HostStatsCollector(GeventActor):
    def on_receive(self, message: Any) -> Iterator:
        collection_methods = [
            HostCollectorPlugin.get_cpu_percentage,
            HostCollectorPlugin.get_fs_metrics,
            HostCollectorPlugin.get_ram_metrics,
        ]
        metrics = map(lambda func: func(), collection_methods)
        return reduce(lambda a, b: chain(a, b), metrics)


class HostCollectorPlugin(CollectorPlugin):
    @classmethod
    def get_cpu_percentage(cls) -> Iterator:
        cpu_percentage = psutil.cpu_percent()
        timestamp = time.time()
        if cpu_percentage != 0.0:
            collecting_metrics = [("host.cpu.percentage", cpu_percentage)]
        else:
            collecting_metrics = []
        return cls._wrap_metrics(collecting_metrics, timestamp)

    @classmethod
    def get_fs_metrics(cls) -> Iterator:
        filesystems = psutil.disk_partitions()
        mapped = map(cls._process_filesystem, filesystems)
        metrics = reduce(lambda a, b: chain(a, b), mapped)
        return metrics

    @classmethod
    def _process_filesystem(cls, filesystem: namedtuple) -> Iterator:
        tags = [f"device:{filesystem.device}"]
        fs_usage = psutil.disk_usage(filesystem.mountpoint)
        timestamp = time.time()
        collecting_metrics = [
            ("host.fs.used", fs_usage.used),
            ("host.fs.free", fs_usage.free),
        ]
        return cls._wrap_metrics(collecting_metrics, timestamp, tags)

    @classmethod
    def get_ram_metrics(cls) -> Iterator:
        memory = psutil.virtual_memory()
        timestamp = time.time()
        collecting_metrics = [
            ("host.memory.used", memory.used),
            ("host.memory.available", memory.available),
        ]
        return cls._wrap_metrics(collecting_metrics, timestamp)
