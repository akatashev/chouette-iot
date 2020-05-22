from abc import ABC
from typing import Iterator, List, Optional, Tuple

from chouette.metrics import WrappedMetric


class CollectorPlugin(ABC):
    @staticmethod
    def _wrap_metrics(
        metrics: List[Tuple[str, float]],
        timestamp: float,
        tags: Optional[List[str]] = None,
        metric_type: str = "gauge",
    ) -> Iterator:
        return map(
            lambda metric: WrappedMetric(
                metric[0], metric_type, metric[1], timestamp, tags
            ),
            metrics,
        )
