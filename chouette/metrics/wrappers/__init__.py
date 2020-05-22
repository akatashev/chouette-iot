"""
chouette.metrics.wrappers
"""
from typing import Optional

from ._metrics_wrapper import MetricsWrapper
from ._simple_wrapper import SimpleWrapper

__all__ = ["SimpleWrapper", "WrappersFactory"]


class WrappersFactory:
    """
    WrapperFactory class creates Metrics Wrapper instances.
    """

    # pylint: disable=too-few-public-methods
    @classmethod
    def get_wrapper(cls, wrapper_name: str) -> Optional[MetricsWrapper]:
        """
        Takes a wrapper name and returns a wrapper instance.

        Args:
            wrapper_name: Name of a wrapper as a string.
        Returns: ActorRef or None.
        """
        if wrapper_name.lower() == "simple":
            return SimpleWrapper()
        return None
