from typing import Optional


class WrappedMetric:
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
        self.timestamp = timestamp
        self.value = value
        self.type = metric_type
        self.tags = tags if tags else []

    def __str__(self):
        return str(self.asdict())

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.__str__()}"

    def asdict(self):
        return {
            "metric": self.metric,
            "tags": self.tags,
            "points": [[self.timestamp, self.value]],
            "type": self.type,
        }


class MergedMetric:
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
        return str(self.asdict())

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.__str__()}"

    def __add__(self, other):
        if self.name != other.name or self.tags != other.tags:
            raise ValueError("MergedMetrics must have the same name and tags.")
        return MergedMetric(
            name=self.name,
            metric_type=self.type,
            values=self.values + other.values,
            timestamps=self.timestamps + other.timestamps,
            tags=self.tags,
        )

    def asdict(self):
        return {
            "name": self.name,
            "tags": self.tags,
            "values": self.values,
            "timestamps": self.timestamps,
            "type": self.type,
        }
