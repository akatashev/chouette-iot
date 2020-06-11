"""
chouette.metrics.plugins.K8sCollector
"""
# pylint: disable=too-few-public-methods
import json
import logging
from itertools import chain
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from pydantic import BaseSettings, ValidationError  # type: ignore
from pykka import ActorDeadError  # type: ignore

from chouette_iot._singleton_actor import SingletonActor
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse
from chouette_iot.metrics import WrappedMetric

__all__ = ["K8sCollector"]

logger = logging.getLogger("chouette-iot")


class K8sCollectorConfig(BaseSettings):
    """
    Environment variables based plugin configuration.

    K8S_STATS_SERVICE_IP is normally the IP of the node, it could be an
    external IP (e.g. 192.168.1.104) or an internal IP (e.g. 10.1.18.1). It's
    better to use an internal IP if your connectivity isn't too good. because
    if you rely on your external IP and connection disappears, the plugin will
    stop collecting metrics. In case of an internal IP that won't happen.

    K8S_STATS_SERVICE_PORT is a port of the Stats Service. By default it's
    10250.

    K8S_CERT_PATH is a path to a client certificate to pass the Stats Server
    authorization.
    In microk8s it's usually /var/snap/microk8s/current/certs/server.crt

    K8S_KEY_PATH is a path to a client key to pass the Stats Server
    authorization.
    In microk8s it's usually /var/snap/microk8s/current/certs/server.key

    K8S_METRICS is a structure that defines what metrics should be sent
    to Datasog.
    """

    k8s_stats_service_ip: str
    k8s_stats_service_port: int = 10250
    k8s_cert_path: str  # Path to server.crt for microk8s
    k8s_key_path: str  # Path to server.key for microk8s
    k8s_metrics: Dict[str, List[str]] = {"pods": ["memory", "cpu"], "node": ["inodes"]}


class K8sCollector(SingletonActor):
    """
    K8sCollector plugin collects stats about K8s node and pods via a K8s
    Stats Service.
    """

    def __init__(self):
        super().__init__()
        try:
            config = K8sCollectorConfig()
            self.k8s_url: str = f"https://{config.k8s_stats_service_ip}:{config.k8s_stats_service_port}/stats/summary"
            self.certs: Tuple[str, str] = (config.k8s_cert_path, config.k8s_key_path)
            self.k8s_metrics: Dict[str, List[str]] = config.k8s_metrics
        except ValidationError:
            self.k8s_url = None
            logger.warning(
                "[%s] Kubernetes configuration is not correct. Metrics won't be collected.",
                self.name,
                exc_info=True,
            )

    def on_receive(self, message: StatsRequest) -> None:
        """
        On StatsRequest message collects K8s pods stats and
        sends them back in a StatsResponse message.

        On any other message does nothing.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest) and self.k8s_url:
            stats = K8sCollectorPlugin.collect_stats(
                self.k8s_url, self.certs, self.k8s_metrics
            )
            if hasattr(message.sender, "tell"):
                try:
                    message.sender.tell(StatsResponse(self.name, stats))
                except ActorDeadError:
                    logger.warning(
                        "[%s] Requester is stopped. Dropping message.", self.name
                    )


class K8sCollectorPlugin(CollectorPlugin):
    """
    K8sCollectorPlugin handles collecting data from K8s and wrapping it
    into iterator of WrappedMetrics.
    """

    @classmethod
    def collect_stats(
        cls, url: str, certs: Tuple[str, str], to_collect: Dict[str, List[str]]
    ) -> Iterator[WrappedMetric]:
        """
        Gathers all the requested metrics and wraps them into an Iterator.

        If it couldn't get any data from K8s - returns an empty Iterator.

        Args:
            url: URL of a Stats Service to gather data from.
            certs: Tuple with paths of a client cert and a client key to
                   authorize.
            to_collect: Dict with metrics configuration.
        Returns: Iterator over WrappedMetric objects.
        """
        raw_metrics_dict = cls._get_raw_metrics(url, certs)
        if not raw_metrics_dict:
            return iter([])
        stats = [
            cls._parse_node_metrics(raw_metrics_dict, to_collect.get("node", [])),
            cls._parse_pods_metrics(raw_metrics_dict, to_collect.get("pods", [])),
        ]
        return chain.from_iterable(stats)

    @classmethod
    def _parse_node_metrics(
        cls, raw_metrics: Dict[str, Any], to_collect: List[str]
    ) -> Iterator[WrappedMetric]:
        """
        Gets a dict with a raw K8s Stats Service response and produces
        metrics based on its "node" content.

        This method doesn't return all the metrics that could be found
        in a Stats Service response.
        That's just a subset that can be extended with more fs data
        or networking data if necessary.

        Args:
            raw_metrics: Dict containing K8s Stats Service response.
            to_collect: List of node metrics to return.
        Returns: Iterator over WrappedMetric objects.
        """
        node = raw_metrics.get("node")
        if not node:
            return iter([])
        node_name = node.get("nodeName")
        tags = [f"node_name:{node_name}"]
        cpu = node.get("cpu", {}) if "cpu" in to_collect else {}
        ram = node.get("memory", {}) if "ram" in to_collect else {}
        network = node.get("network", {}) if "network" in to_collect else {}
        f_system = node.get("fs", {}) if "filesystem" in to_collect else {}
        inodes = (
            node.get("fs", {}).get("inodesFree") if "inodes" in to_collect else None
        )
        prefix = "Chouette.k8s.node"
        stats = [
            (f"{prefix}.cpu.usageNanoCores", cpu.get("usageNanoCores")),
            (f"{prefix}.cpu.usageCoreNanoSeconds", cpu.get("usageCoreNanoSeconds")),
            (f"{prefix}.memory.rssBytes", ram.get("rssBytes")),
            (f"{prefix}.memory.usageBytes", ram.get("usageBytes")),
            (f"{prefix}.memory.availableBytes", ram.get("availableBytes")),
            (f"{prefix}.memory.workingSetBytes", ram.get("workingSetBytes")),
            (f"{prefix}.network.rxBytes", network.get("rxBytes")),
            (f"{prefix}.network.txBytes", network.get("txBytes")),
            (f"{prefix}.fs.inodesFree", inodes),
            (f"{prefix}.fs.availableBytes", f_system.get("availableBytes")),
            (f"{prefix}.fs.usedBytes", f_system.get("usedBytes")),
        ]
        collected_metrics = [(name, value) for name, value in stats if value]
        return cls._wrap_metrics(collected_metrics, tags=tags)

    @classmethod
    def _parse_pods_metrics(
        cls, raw_metrics: Dict[str, Any], to_collect: List[str]
    ) -> Iterator[WrappedMetric]:
        """
        Just a wrapper for a _parse_pod_metrics method.

        Takes a raw_metrics dict, gets a list of pods from it and generate
        an Iterator of pods metrics using a _parse_pod_metrics method.

        Args:
            raw_metrics: Dict containing K8s Stats Service response.
            to_collect: List of pod metrics to return.
        Returns: Iterator over WrappedMetric objects.
        """
        pods: List[Dict[str, Any]] = raw_metrics.get("pods", [])
        metrics = [
            cls._parse_pod_metrics(pod_metrics, to_collect) for pod_metrics in pods
        ]
        return chain.from_iterable(metrics)

    @classmethod
    def _parse_pod_metrics(
        cls, pod: Dict[str, Any], to_collect: List[str]
    ) -> Iterator[WrappedMetric]:
        """
        Gets a dict with a pod stats description and casts it into an
        Iterator over metrics.

        That's just a tiny subset of metrics returned by K8s Stats Service,
        it can be expanded if necessary.

        Args:
            pod: Dict containing pod data from K8s Stats Service response.
            to_collect: List of pod metrics to return.
        Returns: Iterator over WrappedMetric objects.
        """
        pod_ref: Optional[Dict[str, str]] = pod.get("podRef")
        if not pod_ref:
            return iter([])
        tags = [
            f"namespace:{pod_ref.get('namespace')}",
            f"pod_name:{pod_ref.get('name')}",
        ]
        cpu = pod.get("cpu", {}) if "cpu" in to_collect else {}
        ram = pod.get("memory", {}) if "memory" in to_collect else {}
        network = pod.get("network", {}) if "network" in to_collect else {}
        prefix = "Chouette.k8s.pod"
        stats = [
            (f"{prefix}.cpu.usageNanoCores", cpu.get("usageNanoCores")),
            (f"{prefix}.memory.rssBytes", ram.get("rssBytes")),
            (f"{prefix}.memory.usageBytes", ram.get("usageBytes")),
            (f"{prefix}.network.rxBytes", network.get("rxBytes")),
            (f"{prefix}.network.txBytes", network.get("txBytes")),
        ]
        collected_metrics = [(name, value) for name, value in stats if value]
        return cls._wrap_metrics(collected_metrics, tags=tags)

    @staticmethod
    def _get_raw_metrics(url: str, certs: Tuple[str, str]) -> Dict[str, Any]:
        """
        Tries to connect to a K8s Stats Service, receive its response and cast
        it to a dict.

        If for any reason it wasn't able to do this, it returns an empty dict.

        Args:
            url: URL of a Stats Service to gather data from.
            certs: Tuple with paths of a client cert and a client key to
                   authorize.
        Returns: Dict containing K8s Stats Service output.
        """
        try:
            response = requests.get(url, cert=certs, verify=False, timeout=5)
        except (requests.RequestException, IOError) as error:
            logger.warning(
                "[K8sCollector] Could not collect data from %s due to %s", url, error
            )
            return {}
        if response.status_code != 200:
            logger.warning(
                "[K8sCollector] K8s returned %s: %s.",
                response.status_code,
                response.text,
            )
            return {}
        try:
            metrics_dict = response.json()
        except json.JSONDecodeError as error:
            logger.warning(
                "[K8sCollector] K8s returned non-JSON response. Error: %s", error
            )
            return {}
        return metrics_dict
