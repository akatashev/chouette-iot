"""
chouette.metrics.plugins.TegrastatsCollector

Stats collector for Nvidia Jetson Devices.
"""
# pylint: disable=too-few-public-methods
import logging
import re
from itertools import chain
from subprocess import Popen, PIPE
from typing import Iterator, List

from pydantic import BaseSettings  # type: ignore
from pykka import ActorDeadError  # type: ignore

from chouette_iot._singleton_actor import SingletonActor
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse

__all__ = ["TegrastatsCollector"]

logger = logging.getLogger("chouette-iot")


class TegrastatsConfig(BaseSettings):
    """
    Environment variables based configuration.

    It specifies what metrics this plugin should collect and a path to a
    Tegrastats executable.
    By default all metrics are listed here, but it's possible to request
    a subset of them via an environment variable.
    """

    tegrastats_metrics: List[str] = ["ram", "temp"]
    tegrastats_path: str = "/usr/bin/tegrastats"


class TegrastatsCollector(SingletonActor):
    """
    Actor that collects stats from an Nvidia Tegrastats utility.

    NB: Collectors MUST interact with plugins via `tell` pattern.
        `ask` pattern will return None.
    """

    def __init__(self):
        super().__init__()

        config = TegrastatsConfig()
        self.metrics = config.tegrastats_metrics
        self.path = config.tegrastats_path

    def on_receive(self, message):
        """
        On StatsRequest message collects specified metrics and
        sends them back in a StatsResponse message.

        On any other message does nothing.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest):
            stats = TegrastatsPlugin.collect_stats(self.path, self.metrics)
            if hasattr(message.sender, "tell"):
                try:
                    message.sender.tell(StatsResponse(self.name, stats))
                except ActorDeadError:
                    logger.warning(
                        "[%s] Requester is stopped. Dropping message.", self.name
                    )


class TegrastatsPlugin(CollectorPlugin):
    """
    CollectorPlugin that handles RAM and Temperature metrics from Tegrastats.

    Built around Tegrastats application that is a part of L4T Nvidia project:
    https://docs.nvidia.com/jetson/l4t/index.html
    """

    @classmethod
    def collect_stats(cls, path: str, metrics_to_collect: List[str]) -> Iterator:
        """
        Collects requested stats from Tegrastats.

        Args:
            path: Path to a Tegrastats executable.
            metrics_to_collect: List of metric types to collect.
        Returns: Iterator over WrappedMetric objects.
        """
        tegrastats_methods = {
            "ram": cls._get_ram_metrics,
            "temp": cls._get_temp_metrics,
        }
        raw_string = cls._get_raw_metrics_string(path)
        methods = filter(None, map(tegrastats_methods.get, metrics_to_collect))
        metrics = [method(raw_string) for method in methods]
        return chain.from_iterable(metrics)

    @classmethod
    def _get_temp_metrics(cls, raw_string: str) -> Iterator:
        """
        Gets temperature of different zones from Tegrastats output.

        Every zone is being sent as a tag for a 'temperature' metric.
        PMIC zone that always shows 100C is dropped.

        Args:
            raw_string: Tegrastats output as a raw sting.
        Returns: Iterator over WrappedMetric objects.
        """
        pattern = re.compile(r"\b(\w+)@([0-9.]+)C\b")
        stats = re.findall(pattern, raw_string)
        metrics = [
            cls._wrap_metrics(
                [("Chouette.tegrastats.temperature", float(value))],
                tags=[f"zone:{zone}"],
            )
            for zone, value in stats
            if zone != "PMIC"
        ]
        return chain.from_iterable(metrics)

    @classmethod
    def _get_ram_metrics(cls, raw_string: str) -> Iterator:
        """
        Gets RAM stats from Tegrastats output.

        It's expected to be less precise than analogous statistics from
        the HostCollectorPlugin, because Tegrastats returns values in MBs
        and to get values in bytes we need to convert these MBs into bytes.

        Args:
            raw_string: Tegrastats output as a raw sting.
        Returns: Iterator over WrappedMetric objects.
        """
        pattern = re.compile(r"\bRAM\b (\d+)/(\d+)MB")
        data = re.findall(pattern, raw_string)
        if not data:
            return iter([])
        used, total = data.pop()
        used_bytes = float(used) * 1024 * 1024
        free_bytes = (float(total) - float(used)) * 1024 * 1024
        collecting_metrics = [
            ("Chouette.tegrastats.ram.used", used_bytes),
            ("Chouette.tegrastats.ram.free", free_bytes),
        ]
        return cls._wrap_metrics(collecting_metrics)

    @staticmethod
    def _get_raw_metrics_string(path: str) -> str:
        """
        Runs Tegrastats and gets a single raw string with stats.

        Args:
            path: Path to a Tegrastats executable.
        Returns: String with raw metrics.
        """
        try:
            ts_proc = Popen(path, stdout=PIPE)
            stdout = ts_proc.stdout
            if stdout:
                data_string = stdout.readline().decode()
            else:
                data_string = ""  # pragma: no cover
            ts_proc.kill()
        except (FileNotFoundError, PermissionError):
            data_string = ""
        return data_string
