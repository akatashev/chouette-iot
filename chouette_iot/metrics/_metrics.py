"""
Metrics classes to handle metrics processing.
"""
# pylint: disable=too-few-public-methods
import time
from typing import Any, Dict, List, Optional

__all__ = ["MergedMetric", "WrappedMetric"]


class Metric:
    """
    Base parent class for all the metrics.
    """

    __slots__ = ["interval", "metric", "type", "tags"]

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

    def __eq__(self, other: object) -> bool:
        """
        Metrics are considered equal if their dicts are equal.

        Args:
            other: Another SingleMetric object to compare.
        Return: Whether their dicts are equal.
        """
        if isinstance(other, Metric):
            return self.asdict() == other.asdict()
        return False

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
        return sorted(tags_list)


class MergedMetric(Metric):
    """
    MergedMetrics is a single object, that contains aggregated values and
    timestamps of a number of metrics of the same name, type and tags,
    collected during the same `aggregate_interval` period.

    This aggregated data is a raw data that is being transformed by a
    MetricsWrapper object to create WrappedMetrics, ready for release to
    Datadog.

    They have separated `values` and `timestamps` fields and their tags are
    dicts. But when a MergedMetric is generated, it creates an `s_tags`
    property that represents the same tags in a form that should be passed
    to a WrappedMetric - as a list of strings. `asdict` method also returns
    a dict with these processed tags.

    Self.id is a unique identifier of a metric combined of its name, type
    and tags. Only MergedMetrics with the same id can be merged together.
    """

    __slots__ = ["id", "timestamps", "values"]

    def __init__(self, **kwargs: Any):
        self.metric: str = kwargs["metric"]
        self.type: str = kwargs["type"]
        self.values: List[Any] = kwargs.get("values", [])
        self.timestamps: List[float] = kwargs.get("timestamps", [])
        self.interval = kwargs.get("interval", 10)
        tags = kwargs.get("tags")
        self.tags: Dict[str, str] = tags if tags else {}
        self.id: str = f"{self.metric}_{self.type}{'_'.join(self._stringify_tags(self.tags))}"

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
            "tags": self.tags,
            "values": self.values,
            "timestamps": self.timestamps,
            "type": self.type,
            "interval": self.interval,
        }


class WrappedMetric(Metric):
    """
    WrappedMetric is a metric that is ready to be released.

    Its timestamp and value are calculated and they form the only data
    point that this metric contains. It usually represents a calculated
    value of some metric for some period of time.

    One MergedMetric, being processed by a MetricsWrapper can produce
    a number of WrappedMetrics.

    Unlike MergedMetric, WrappedMetric's tags are not a dict but a list:
    Tags {"tag1": "value1", "tag2": "value2"} should be merged into a list
    ["tag1:value1", "tag2:value2"] before creating a WrappedMetric.

    For aggregated metrics this job is being performed by a MetricsWrapper,
    but for WrappedMetrics produced by CollectionPlugins it should be done
    by a developer.
    """

    __slots__ = ["timestamp", "value"]

    def __init__(self, **kwargs: Any):
        self.metric = kwargs["metric"]
        self.type = kwargs["type"]
        self.value = kwargs["value"]
        self.interval: Optional[int] = kwargs.get("interval")
        timestamp = kwargs.get("timestamp")
        tags = kwargs.get("tags")
        self.timestamp = timestamp if timestamp else time.time()
        self.tags: List[str] = self._stringify_tags(tags) if tags else []

    def asdict(self):
        """
        Returns a dict form of the metric that is ready to be cast
        to JSON and stored for releasing.

        Return: Dict that represents the metric.
        """
        dict_representation = {
            "metric": self.metric,
            "tags": self.tags,
            "points": [[self.timestamp, self.value]],
            "type": self.type,
        }
        if self.interval:
            dict_representation.update({"interval": self.interval})
        return dict_representation

    def __hash__(self):
        """
        That's a dirty hack needed to avoid duplicated WrappedMetrics for
        HostStatsCollector.

        Return: Hash of a string representation of the metric.
        """
        return hash(self.__str__())
