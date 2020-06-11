"""
chouette.metrics.plugins.messages

Contains messages that Collectors and CollectorPlugins use to communicate.
"""
# pylint: disable=too-few-public-methods
from typing import Iterator

from pykka import ActorRef  # type: ignore


__all__ = ["StatsRequest", "StatsResponse"]


class StatsRequest:
    """
    StatsRequest is a message that a Collector sends to a Plugin.

    It requests that Plugin to gather stats and send them back as a
    StatsResponse message.

    To give the Plugin a chance to send its stats back, this message
    contains a `sender` ActorRef parameter.
    """

    __slots__ = ["sender"]

    def __init__(self, sender: ActorRef):
        """
        Args:
            sender: ActorRef of a Collector that requested stats.
        """
        self.sender = sender

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"<{self.__class__.__name__} from {self.sender}>"


class StatsResponse:
    """
    StatsResponse is a message that a Plugin sends to a Collector.

    It's being produces as a response on a StatsRequest message.

    Since interaction between Collectors and Plugins is completely
    non-blocking, the Collector won't know what plugin sent it a
    message, to this message contains the Plugin's name in its
    `producer` attribute.
    """

    __slots__ = ["producer", "stats"]

    def __init__(self, producer: str, stats: Iterator):
        """
        Args:
            producer: Name of a Plugin that produced the message.
            stats: Iterator over WrappedMetric objects.
        """
        self.producer = producer
        self.stats = stats

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"<{self.__class__.__name__} from {self.producer}>"
