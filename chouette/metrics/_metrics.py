"""
Metrics classes to handle metrics processing.
"""
# pylint: disable=too-few-public-methods
import time
from abc import ABC, abstractmethod
from typing import Any, List

__all__ = ["MergedMetric", "RawMetric", "WrappedMetric"]


class SingleMetric(ABC):
    """
    Abstract class for both Wrapped and Raw metrics.
    """

    __slots__ = ["metric", "tags", "timestamp", "value", "type"]

    def __init__(self, **kwargs: Any):
        self.metric = kwargs["metric"]
        self.type = kwargs["type"]
        self.value = kwargs["value"]
        self.timestamp = kwargs.get("timestamp", time.time())
        self.tags = kwargs.get("tags", [])

    def __str__(self):
        return str(self.asdict())

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.__str__()}>"

    def __eq__(self, other) -> bool:
        """
        Metrics are considered equal if their dicts are equal.

        Args:
            other: Another SingleMetric object to compare.
        Return: Whether their dicts are equal.
        """
        if hasattr(other, "asdict"):
            return self.asdict() == other.asdict()
        return False

    @abstractmethod
    def asdict(self):  # pragma: no cover
        """
        Returns a dict form of the metric that is ready to be cast
        to JSON and stored for releasing.

        Return: Dict that represents the metric.
        """
        pass


class WrappedMetric(SingleMetric):
    """
    Wrapped metric is a metric that is ready to be released.

    Its timestamp and value are calculated and they form the only
    data point that this metric contains.
    It usually represents a calculated value of some metric for
    some period of time.
    """

    def asdict(self):
        """
        Returns a dict form of the metric that is ready to be cast
        to JSON and stored for releasing.

        Return: Dict that represents the metric.
        """
        return {
            "metric": self.metric,
            "tags": self.tags,
            "points": [[self.timestamp, self.value]],
            "type": self.type,
        }


class MergedMetric:
    """
    Merged metric is a metric that contains numerous values of a metric
    collected during some period of time.

    Usually it has numerous timestamps and numerous values and these values
    must be somehow processed to generate a WrappedMetric that is ready
    for releasing.

    MetricWrapper class consumes lists of MergedMetrics.
    """

    __slots__ = ["metric", "tags", "timestamps", "values", "type"]

    def __init__(self, **kwargs: Any):
        self.metric = kwargs["metric"]
        self.type = kwargs["type"]
        self.values = kwargs.get("values", [])
        self.timestamps = kwargs.get("timestamps", [])
        self.tags = kwargs.get("tags", [])

    def __str__(self):
        return str(self.asdict())

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.__str__()}>"

    def __add__(self, other):
        """
        That's the Merge operation of a MergedMetric.

        If there are two MergedMetrics with the same name and same tags, we
        need to be able to merge them and to receive a Metric with the same
        type, same tags and same type whose values and timestamps are merged
        values and timestamps of the original metrics.

        Args:
            other: MergedMetric object to merge with this metric.
        Returns: A new metric with merged values and timestamps.
        """
        different_names = self.metric != other.metric
        different_types = self.type != other.type
        different_tags = self.tags != other.tags
        if different_names or different_types or different_tags:
            raise ValueError("Can't merge different metrics.")
        return MergedMetric(
            metric=self.metric,
            type=self.type,
            values=self.values + other.values,
            timestamps=self.timestamps + other.timestamps,
            tags=self.tags,
        )

    def __eq__(self, other) -> bool:
        """
        Metrics are considered equal if their dicts are equal.

        Args:
            other: Another SingleMetric object to compare.
        Return: Whether their dicts are equal.
        """
        if hasattr(other, "asdict"):
            return self.asdict() == other.asdict()
        return False

    def asdict(self):
        """
        Returns: Dict representation of the metric.
        """
        return {
            "metric": self.metric,
            "tags": self.tags,
            "values": self.values,
            "timestamps": self.timestamps,
            "type": self.type,
        }


class RawMetric(SingleMetric):
    """
    Raw metric is a metric that needs to be processed to be released.

    It's used to store self metrics and represents a single metric datapoint.
    Unlike MergedMetric and WrappedMetric, RawMetric contains a list of tags
    where every tag is represented as a dict, not a string. Tags are being
    merged into a string during RawMetric processing by MetricsAggregator.
    """

    def asdict(self):
        """
        Returns a dict form of the metric that is ready to be cast
        to JSON and stored for releasing.

        Return: Dict that represents the metric.
        """
        return {
            "metric": self.metric,
            "tags": self.tags,
            "timestamp": self.timestamp,
            "value": self.value,
            "type": self.type,
        }

    def mergify(self) -> MergedMetric:
        """
        Casts a RawMetric instance into a MergedMetric instance.

        Returns: MergedMetric.
        """
        return MergedMetric(
            metric=self.metric,
            type=self.type,
            values=[self.value],
            timestamps=[self.timestamp],
            tags=self._stringify_tags(),
        )

    def _stringify_tags(self) -> List[str]:
        """
        Takes a list of tags as dicts and casts every dict into a line
        "key:value".

        Returns: List of strings with reformatted tags.
        """
        if not self.tags:
            return []
        try:
            tags_list = [f"{name}:{str(value)}" for name, value in self.tags]
        except (TypeError, ValueError):
            tags_list = []
        return tags_list
