from typing import Optional


class WrappedMetric:
    __slots__ = ["name", "tags", "timestamp", "value", "type"]

    def __init__(
        self,
        name: str,
        metric_type: str,
        value: float,
        timestamp: float,
        tags: Optional[list] = None,
    ):
        self.name = name
        self.timestamp = timestamp
        self.value = value
        self.type = metric_type
        self.tags = tags if tags else []

    def __str__(self):
        return str(self.asdict())

    def asdict(self):
        return {
            "metric": self.name,
            "tags": self.tags,
            "points": [[self.timestamp, self.value]],
            "type": self.type,
        }
