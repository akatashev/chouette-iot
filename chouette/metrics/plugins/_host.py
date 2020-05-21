import time
from typing import List

import psutil
from pykka.gevent import GeventActor

from chouette.metrics import WrappedMetric


class HostCollectingPlugin(GeventActor):
    def on_receive(self, message):
        host_metrics = []
        host_metrics.extend(HostStatsCollector.get_cpu_percentage())
        host_metrics.extend(HostStatsCollector.get_fs_metrics())
        host_metrics.extend(HostStatsCollector.get_ram_metrics())
        return host_metrics


class HostStatsCollector:
    @staticmethod
    def get_cpu_percentage() -> List[WrappedMetric]:
        cpu_percentage = psutil.cpu_percent()
        if cpu_percentage == 0.0:
            return []
        timestamp = time.time()
        metric = WrappedMetric(
            "host.cpu.percentage", "gauge", cpu_percentage, timestamp
        )

        return [metric]

    @staticmethod
    def get_fs_metrics() -> List[WrappedMetric]:
        metrics = []
        filesystems = psutil.disk_partitions()
        for fs in filesystems:
            tags = [f"device:{fs.device}"]
            fs_usage = psutil.disk_usage(fs.mountpoint)
            timestamp = time.time()
            collecting_metrics = {
                "host.fs.used": fs_usage.used,
                "host.fs.free": fs_usage.free,
            }
            for name, value in collecting_metrics.items():
                metrics.append(WrappedMetric(name, "gauge", value, timestamp, tags))

        return metrics

    @staticmethod
    def get_ram_metrics() -> List[WrappedMetric]:
        metrics = []
        memory = psutil.virtual_memory()
        timestamp = time.time()
        collecting_metrics = {
            "host.memory.used": memory.used,
            "host.memory.available": memory.available,
        }
        for name, value in collecting_metrics.items():
            metrics.append(WrappedMetric(name, "gauge", value, timestamp))

        return metrics
