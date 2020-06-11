"""
Redis specific messages, being used to collect stats about Redis queues.
"""
# pylint: disable=too-few-public-methods

from typing import Iterable, Union


class GetHashSizes:
    """
    THis message initiates collection of Redis Hashes sizes.
    """

    __slots__ = ["hashes"]

    def __init__(self, hashes: Iterable[Union[bytes, str]]):
        self.hashes = hashes


class GetRedisQueues:
    """
    This message initiates collection of Redis entities names.
    """

    __slots__ = ["pattern"]

    def __init__(self, pattern: str):
        self.pattern = pattern
