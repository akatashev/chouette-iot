"""
Metrics classes to handle metrics processing.
"""
from typing import Optional


class WrappedMetric:
    """
    Wrapped metric is a metric that is ready to be released.

    Its timestamp and value are calculated and they form the only
    data point that this metric contains.
    It usually represents a calculated value of some metric for
    some period of time.
    """

    __slots__ = ["metric", "tags", "timestamp", "value", "type"]

    def __init__(
        self,
        metric: str,
        metric_type: str,
        value: float,
        timestamp: float,
        tags: Optional[list] = None,
    ):
        self.metric = metric
        self.timestamp = int(timestamp)
        self.value = value
        self.type = metric_type
        self.tags = tags if tags else []

    def __str__(self):
        """
        Returns: A string representation of the metric as a dict.
        """
        return str(self.asdict())

    def __repr__(self):
        """
        Returns: Class name and a string representation of the metric.
        """
        return f"{self.__class__.__name__}: {self.__str__()}"

    def asdict(self):
        """
        Returns a dict form of the metric that is ready to be casted
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

    __slots__ = ["name", "tags", "timestamps", "values", "type"]

    def __init__(
        self,
        name: str,
        metric_type: str,
        values: Optional[list] = None,
        timestamps: Optional[list] = None,
        tags: Optional[list] = None,
    ):
        self.name = name
        self.type = metric_type
        self.values = values if values else []
        self.timestamps = timestamps if timestamps else []
        self.tags = tags if tags else []

    def __str__(self):
        """
        Returns: A string representation of the metric as a dict.
        """
        return str(self.asdict())

    def __repr__(self):
        """
        Returns: Class name and a string representation of the metric.
        """
        return f"{self.__class__.__name__}: {self.__str__()}"

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
        different_names = self.name != other.name
        different_types = self.type != other.type
        different_tags = self.tags != other.tags
        if different_names or different_types or different_tags:
            raise ValueError("Can't merge different metrics.")
        return MergedMetric(
            name=self.name,
            metric_type=self.type,
            values=self.values + other.values,
            timestamps=self.timestamps + other.timestamps,
            tags=self.tags,
        )

    def asdict(self):
        """
        Returns: Dict representation of the metric.
        """
        return {
            "name": self.name,
            "tags": self.tags,
            "values": self.values,
            "timestamps": self.timestamps,
            "type": self.type,
        }