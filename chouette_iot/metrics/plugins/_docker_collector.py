"""
chouette.metrics.plugins.DockerCollector
"""
# pylint: disable=too-few-public-methods
import json
import logging
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, wait
from itertools import chain
from typing import Iterator, List

import requests_unixsocket  # type: ignore
from pydantic import BaseSettings  # type: ignore
from pykka import ActorDeadError  # type: ignore
from requests import RequestException

from chouette_iot._singleton_actor import SingletonActor
from chouette_iot.metrics import WrappedMetric
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse

__all__ = ["DockerCollector"]

logger = logging.getLogger("chouette-iot")


class DockerCollectorConfig(BaseSettings):
    """
    Optional configuration that specifies a path to a docker socket in
    your container.
    """

    docker_socket_path = "/var/run/docker.sock"


class DockerCollector(SingletonActor):
    """
    Docker collector plugin:
    Collects data about docker containers directly from a docker unix socket.
    Should be used if docker-compose or pure docker is used to run software
    on your device.

    NB: Docker socket must be the host docker socket. It must be added as a
    volume to Chouette container.

    Docker-compose file volume addition example:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock

    Default socket path is /var/run/docker.sock
    """

    def __init__(self):
        super().__init__()
        socket_path = DockerCollectorConfig().docker_socket_path
        encoded_socket_path = urllib.parse.quote(socket_path, safe="")
        self.docker_url = f"http+unix://{encoded_socket_path}/containers"

    def on_receive(self, message: StatsRequest) -> None:
        """
        On StatsRequest message collects Docker statistics and
        sends them back in a StatsResponse message.

        On any other message does nothing.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest):
            stats = DockerCollectorPlugin.collect_metrics(self.docker_url)
            if hasattr(message.sender, "tell"):
                try:
                    message.sender.tell(StatsResponse(self.name, stats))
                except ActorDeadError:
                    logger.warning(
                        "[%s] Requester is stopped. Dropping message.", self.name
                    )


class DockerCollectorPlugin(CollectorPlugin):
    """
    Companion object for DockerCollector Actor to abstract data collection
    methods into a separated object not handled by Pykka.
    """

    @classmethod
    def collect_metrics(cls, docker_url: str) -> Iterator[WrappedMetric]:
        """
        Facade function of DockerCollectorPlugin.

        Takes a docker URL that includes path to a docker socket file and
        returns an iterator over collected metrics.

        Since there is no single stats endpoint for all the containers, this
        method uses ThreadPoolExecutor to request data about all containers
        in parallel.

        Args:
            docker_url: Docker URL for requests-unixsocket library.
        Returns: Iterator over WrappedMetric objects.
        """
        ids = cls._get_containers_ids(docker_url)
        if not ids:
            return iter([])
        # Generating futures:
        with ThreadPoolExecutor(max_workers=len(ids)) as executor:
            futures = [
                executor.submit(cls._get_container_stats, container_id, docker_url)
                for container_id in ids
            ]
        # Waiting till all the futures are done:
        wait(futures)
        stats = [future.result() for future in futures]

        return chain.from_iterable(stats)

    @classmethod
    def _get_containers_ids(cls, docker_url: str) -> List[str]:
        """
        Connects to a docker socket file, gets a list of all running
        containers and extracts their ids for individual container stats
        requests.

        Args:
            docker_url: Docker URL for requests-unixsocket library.
        Returns: List of container Ids.
        """
        try:
            containers = requests_unixsocket.get(f"{docker_url}/json").json()
        except (TypeError, RequestException, json.JSONDecodeError, IOError) as error:
            logger.warning(
                "[DockerCollector]: Could not get a list of containers due to: %s",
                error,
            )
            return []
        ids = [container["Id"] for container in containers]
        return ids

    @classmethod
    def _get_container_stats(
        cls, container_id: str, docker_url: str
    ) -> Iterator[WrappedMetric]:
        """
        Connects to a docker socket file and collects stats about a specified
        container.
        Usually this process takes around a second, so that's a blocking
        action.
        `stream=false` parameter is used to specify that we just want to get
        a single stats observation without being connected to the socket all
        the time and receiving actual information as a stream. Without it
        DockerCollector would block forever on it first successful stats
        request.

        Args:
            docker_url: Docker URL for requests-unixsocket library.
        Returns: Iterator over WrappedMetric objects.
        """
        try:
            raw_stats = requests_unixsocket.get(
                f"{docker_url}/{container_id}/stats?stream=false"
            ).json()
        except (TypeError, RequestException, json.JSONDecodeError, IOError):
            logger.warning(
                "[DockerCollector]: Could not get stats for a container %s.",
                container_id,
                exc_info=True,
            )
            return iter([])
        # Extracting data from raw stats:
        container_name = raw_stats["name"][1:]
        memory = raw_stats.get("memory_stats", {})
        cpu = raw_stats.get("cpu_stats", {}).get("cpu_usage", {})
        # Generating metrics:
        tags = [f"container:{container_name}"]
        stats = [
            ("Chouette.docker.memory.usage", memory.get("usage")),
            ("Chouette.docker.cpu.usage", cpu.get("usage_in_kernelmode")),
        ]
        # Filtering possible Nones:
        collected_metrics = [(name, value) for name, value in stats if value]
        return cls._wrap_metrics(collected_metrics, tags=tags)
