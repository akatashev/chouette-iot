from typing import Iterator

from pykka import ActorRef


class StatsRequest:
    __slots__ = ["sender"]

    def __init__(self, sender: ActorRef):
        self.sender = sender


class StatsResponse:
    __slots__ = ["producer", "stats"]

    def __init__(self, producer: str, stats: Iterator):
        self.producer = producer
        self.stats = stats
