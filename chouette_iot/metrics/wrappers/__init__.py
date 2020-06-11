"""
chouette.metrics.wrappers
"""
# pylint: disable=too-few-public-methods
from typing import Optional

from ._metrics_wrapper import MetricsWrapper
from ._simple_wrapper import SimpleWrapper
from ._datadog_wrapper import DatadogWrapper

__all__ = ["SimpleWrapper", "WrappersFactory", "DatadogWrapper"]


class WrappersFactory:
    """
    WrapperFactory class creates Metrics Wrapper instances.
    """

    wrapper_classes = {"simple": SimpleWrapper, "datadog": DatadogWrapper}

    @classmethod
    def get_wrapper(cls, wrapper_name: str) -> Optional[MetricsWrapper]:
        """
        Takes a wrapper name and returns a wrapper instance.

        Args:
            wrapper_name: Name of a wrapper as a string.
        Returns: ActorRef or None.
        """
        wrapper_class = cls.wrapper_classes.get(wrapper_name)
        wrapper = wrapper_class() if wrapper_class else None
        return wrapper
