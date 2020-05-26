"""
Metrics classes to handle metrics processing.
"""
# pylint: disable=too-few-public-methods
import time
from typing import Any, Dict, List

__all__ = ["MergedMetric", "RawMetric", "WrappedMetric"]


class Metric:
    """
    Base parent class for all the metrics.
    """

    __slots__ = ["metric", "type", "tags"]

    def asdict(self):  # pragma: no cover
        """
        Returns a dict form of the metric that is ready to be cast
        to JSON and stored for releasing.

        Return: Dict that represents the metric.
        """
        raise NotImplementedError("Use a concrete Metric class.")

    def __str__(self):
        return str(self.asdict())

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self.__str__()}>"

    def __eq__(self, other: "Metric") -> bool:
        """
        Metrics are considered equal if their dicts are equal.

        Args:
            other: Another SingleMetric object to compare.
        Return: Whether their dicts are equal.
        """
        if isinstance(other, Metric):
            return self.asdict() == other.asdict()
        return False


class MergedMetric(Metric):
    """
    Merged metric is a metric that contains numerous values of a metric
    collected during some period of time.

    Usually it has numerous timestamps and numerous values and these values
    must be somehow processed to generate a WrappedMetric that is ready
    for releasing.

    MetricWrapper class consumes lists of MergedMetrics.

    Self.id is a unique identifier of a metric combined of its name, type
    and tags. Only MergedMetrics with the same id can be merged together.
    """

    __slots__ = ["values", "timestamps", "id"]

    def __init__(self, **kwargs: Any):
        self.metric = kwargs["metric"]
        self.type = kwargs["type"]
        self.values = kwargs.get("values", [])
        self.timestamps = kwargs.get("timestamps", [])
        self.tags = kwargs.get("tags", {})
        self.id = (
            f"{self.metric}_{self.type}{'_'.join(self._stringify_tags(self.tags))}"
        )

    @staticmethod
    def _stringify_tags(tags: Dict[str, str]) -> List[str]:
        """
        Takes a dict of tags and casts every dict into a line
        "key:value".

        Returns: List of strings with reformatted tags.
        """
        try:
            tags_list = [f"{name}:{str(value)}" for name, value in tags.items()]
        except (AttributeError, TypeError, ValueError):
            tags_list = []
        return tags_list

    def __add__(self, other: "MergedMetric"):
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
        if self.id != other.id:
            raise ValueError("Can't merge different metrics.")
        return MergedMetric(
            metric=self.metric,
            type=self.type,
            values=self.values + other.values,
            timestamps=self.timestamps + other.timestamps,
            tags=self.tags,
        )

    def asdict(self):
        """
        Returns: Dict representation of the metric.
        """
        return {
            "metric": self.metric,
            "tags": self._stringify_tags(self.tags),
            "values": self.values,
            "timestamps": self.timestamps,
            "type": self.type,
        }


class SingleMetric(Metric):
    """
    Parent class for both Wrapped and Raw metrics.
    """

    __slots__ = ["value", "timestamp"]

    def __init__(self, **kwargs: Any):
        self.metric = kwargs["metric"]
        self.type = kwargs["type"]
        self.value = kwargs["value"]
        self.timestamp = kwargs.get("timestamp", time.time())


class WrappedMetric(SingleMetric):
    """
    Wrapped metric is a metric that is ready to be released.

    Its timestamp and value are calculated and they form the only
    data point that this metric contains.
    It usually represents a calculated value of some metric for
    some period of time.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.tags = kwargs.get("tags", [])

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


class RawMetric(SingleMetric):
    """
    Raw metric is a metric that needs to be processed to be released.

    It's used to store self metrics and represents a single metric datapoint.
    Unlike MergedMetric and WrappedMetric, RawMetric contains a list of tags
    where every tag is represented as a dict, not a string. Tags are being
    merged into a string during RawMetric processing by MetricsAggregator.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.tags = kwargs.get("tags", {})

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
