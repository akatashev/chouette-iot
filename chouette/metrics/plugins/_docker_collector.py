"""
chouette.metrics.plugins.DockerCollector
"""
# pylint: disable=too-few-public-methods
import logging
from concurrent.futures import ThreadPoolExecutor, wait
from itertools import chain
from typing import Iterator, List

import requests_unixsocket  # type: ignore
from pykka import ActorDeadError  # type: ignore
from requests import RequestException

from chouette._singleton_actor import SingletonActor
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse

__all__ = ["DockerCollector"]

logger = logging.getLogger("chouette")


class DockerCollector(SingletonActor):
    def on_receive(self, message: StatsRequest) -> None:
        """
        On StatsRequest message collects Docket statistics and
        sends them back in a StatsResponse message.

        On any other message does nothing.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest):
            stats = DockerCollectorPlugin.collect_container_metrics()
            if hasattr(message.sender, "tell"):
                try:
                    message.sender.tell(StatsResponse(self.name, stats))
                except ActorDeadError:
                    logger.warning(
                        "[%s] Requester is stopped. Dropping message.", self.name
                    )


class DockerCollectorPlugin(CollectorPlugin):
    docker_path = "http+unix://%2Fvar%2Frun%2Fdocker.sock/containers"

    @classmethod
    def collect_container_metrics(cls) -> Iterator:
        ids = cls._get_container_ids()
        if not ids:
            return iter([])
        with ThreadPoolExecutor(max_workers=len(ids)) as executor:
            futures = [executor.submit(cls._get_container_stats, ids)]
        wait(futures)
        stats = [future.result() for future in futures]
        return chain.from_iterable(stats)

    @classmethod
    def _get_container_ids(cls) -> List[str]:
        try:
            containers = requests_unixsocket.get(f"{cls.docker_path}/json").json()
        except (TypeError, RequestException) as error:
            logger.warning(
                "[DockerCollector]: Could not get a list of containers due to: %s",
                error,
            )
            return []
        ids = [container["Id"] for container in containers]
        return ids

    @classmethod
    def _get_container_stats(cls, cont_id: str) -> Iterator:
        try:
            raw_stats = requests_unixsocket.get(
                f"{cls.docker_path}/{cont_id}/stats?stream=false"
            ).json()
            container_name = raw_stats["name"][1:]
        except (TypeError, RequestException, KeyError) as error:
            logger.warning(
                "[DockerCollector]: Could not get stats for a container %s.",
                cont_id,
                exc_info=True,
            )
            return iter([])
        tags = [f"container:{container_name}"]
        memory = raw_stats.get("memory_stats", {})
        cpu = raw_stats.get("cpu_stats", {}).get("cpu_usage", {})
        stats = [
            ("Chouette.docker.memory.usage", memory.get("usage")),
            ("Chouette.docker.cpu.usage", cpu.get("total_usage")),
        ]
        collected_metrics = [(name, value) for name, value in stats if value]
        return cls._wrap_metrics(collected_metrics, tags=tags)
