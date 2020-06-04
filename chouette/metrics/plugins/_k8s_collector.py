"""
chouette.metrics.plugins.K8sCollector
"""
# pylint: disable=too-few-public-methods
import json
import logging
from itertools import chain
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from pydantic import BaseSettings
from pykka import ActorDeadError  # type: ignore

from chouette._singleton_actor import SingletonActor
from ._collector_plugin import CollectorPlugin
from .messages import StatsRequest, StatsResponse

__all__ = ["K8sCollector"]

logger = logging.getLogger("chouette")


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

    K8S_METRICS is a list of metrics to collect. By default it's just pods,
    because node information can be collected via other plugins and it's
    likely to be more accurate.
    """

    k8s_stats_service_ip: str
    k8s_stats_service_port: int = 10250
    k8s_cert_path: str  # Path to server.crt for microk8s
    k8s_key_path: str  # Path to server.key for microk8s
    k8s_metrics: List[str] = ["pods"]


class K8sCollector(SingletonActor):
    """
    K8sCollector plugin collects stats about K8s node and pods via a K8s
    Stats Service.
    """

    def __init__(self):
        super().__init__()

        config = K8sCollectorConfig()
        self.k8s_url: str = f"https://{config.k8s_stats_service_ip}:" f"{config.k8s_stats_service_port}/stats/summary"
        self.certs: Tuple[str, str] = (config.k8s_cert_path, config.k8s_key_path)
        self.k8s_metrics: List[str] = config.k8s_metrics

    def on_receive(self, message: StatsRequest) -> None:
        """
        On StatsRequest message collects K8s pods stats and
        sends them back in a StatsResponse message.

        On any other message does nothing.

        Args:
            message: Expected to be a StatsRequest message.
        """
        logger.debug("[%s] Received %s.", self.name, message)
        if isinstance(message, StatsRequest):
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
        cls, url: str, certs: Tuple[str, str], metrics: List[str]
    ) -> Iterator:
        """
        Gathers all the requested metrics and wraps them into an Iterator.

        If it couldn't get any data from K8s - returns an empty Iterator.

        Args:
            url: URL of a Stats Service to gather data from.
            certs: Tuple with paths of a client cert and a client key to
                   authorize.
            metrics: List of types of metrics to collect.
        Returns: Iterator over WrappedMetric objects.
        """
        methods = {"node": cls._parse_node_metrics, "pods": cls._parse_pods_metrics}
        raw_metrics_dict = cls._get_raw_metrics(url, certs)
        if not raw_metrics_dict:
            return iter([])
        stats = [
            methods[metric](raw_metrics_dict)
            for metric in metrics
            if methods.get(metric)
        ]
        return chain.from_iterable(stats)

    @classmethod
    def _parse_node_metrics(cls, raw_metrics: Dict[str, Any]) -> Iterator:
        """
        Gets a dict with a raw K8s Stats Service response and produces
        metrics based on its "node" content.

        This method doesn't return all the metrics that could be found
        in a Stats Service response.
        That's just a subset that can be extended with more fs data
        or networking data if necessary.

        Args:
            raw_metrics: Dict containing K8s Stats Service response.
        Returns: Iterator over WrappedMetric objects.
        """
        prefix = "Chouette.k8s.node"
        node_metrics = raw_metrics.get("node", {})
        tags = [f"node_name:{node_metrics.get('nodeName')}"]
        cpu = node_metrics.get("cpu", {})
        ram = node_metrics.get("memory", {})
        network = node_metrics.get("network", {})
        filesystem = node_metrics.get("fs", {})
        collected_metrics = filter(
            lambda pair: pair[1],
            [
                (f"{prefix}.cpu.usageNanoCores", cpu.get("usageNanoCores")),
                (f"{prefix}.cpu.usageCoreNanoSeconds", cpu.get("usageCoreNanoSeconds")),
                (f"{prefix}.memory.rssBytes", ram.get("rssBytes")),
                (f"{prefix}.memory.usageBytes", ram.get("usageBytes")),
                (f"{prefix}.memory.availableBytes", ram.get("availableBytes")),
                (f"{prefix}.memory.workingSetBytes", ram.get("workingSetBytes")),
                (f"{prefix}.network.rxBytes", network.get("rxBytes")),
                (f"{prefix}.network.txBytes", network.get("txBytes")),
                (f"{prefix}.fs.inodesFree", filesystem.get("inodesFree")),
                (f"{prefix}.fs.availableBytes", filesystem.get("availableBytes")),
                (f"{prefix}.fs.usedBytes", filesystem.get("usedBytes")),
            ],
        )
        return cls._wrap_metrics(list(collected_metrics), tags=tags)

    @classmethod
    def _parse_pods_metrics(cls, raw_metrics: Dict[str, Any]) -> Iterator:
        """
        Just a wrapper for a _parse_pod_metrics method.

        Takes a raw_metrics dict, gets a list of pods from it and generate
        an Iterator of pods metrics using a _parse_pod_metrics method.

        Args:
            raw_metrics: Dict containing K8s Stats Service response.
        Returns: Iterator over WrappedMetric objects.
        """
        pods: List[Dict[str, Any]] = raw_metrics.get("pods", [])
        metrics = [cls._parse_pod_metrics(pod_metrics) for pod_metrics in pods]
        return chain.from_iterable(metrics)

    @classmethod
    def _parse_pod_metrics(cls, pod_metrics: Dict[str, Any]) -> Iterator:
        """
        Gets a dict with a pod stats description and casts it into an
        Iterator over metrics.

        That's just a tiny subset of metrics returned by K8s Stats Service,
        it can be expanded if necessary.

        Args:
            pod_metrics: Dict containing pod data from K8s Stats Service response.
        Returns: Iterator over WrappedMetric objects.
        """
        prefix = "Chouette.k8s.pod"
        pod_ref: Optional[Dict[str, str]] = pod_metrics.get("podRef")
        if not pod_ref:
            return iter([])
        tags = [
            f"namespace:{pod_ref.get('namespace')}",
            f"pod_name:{pod_ref.get('name')}",
        ]
        cpu = pod_metrics.get("cpu", {})
        ram = pod_metrics.get("memory", {})
        collected_metrics = filter(
            lambda pair: pair[1],
            [
                (f"{prefix}.cpu.usageNanoCores", cpu.get("usageNanoCores")),
                (f"{prefix}.memory.rssBytes", ram.get("rssBytes")),
                (f"{prefix}.memory.usageBytes", ram.get("usageBytes")),
            ],
        )
        return cls._wrap_metrics(list(collected_metrics), tags=tags)

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
