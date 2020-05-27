"""
chouette.storages.messages module

This file describes objects that other actors should use as messages to
communicate to Storage actors.

These messages are being sent from clients to a Storage.
Both `ask` and `tell` patterns can be used to send them, but since storing
is a crucial part of any data processing, it's heavily recommended to use
`ask` pattern while communicating to Storages.
"""
# pylint: disable=too-few-public-methods
from typing import Iterable, List

__all__ = [
    "CleanupOutdatedRecords",
    "CollectKeys",
    "CollectValues",
    "DeleteRecords",
    "StoreRecords",
]


class CleanupOutdatedRecords:
    """
    Datadog rejects metrics older than 4 hours so there is no sense in
    trying to dispatch them.

    This message is being sent by a Sender actor to cleanup too old
    metrics and to let Sender dispatch only valid metrics.

    It cleans only ready to dispatch ("wrapped") records, so there is
    no "wrapped" flag that all other messages have.
    """

    __slots__ = ["data_type", "ttl", "wrapped"]

    def __init__(self, data_type: str, wrapped: bool, ttl: int = 14400):
        """
        Args:
            data_type: Type of data to cleanup. E.g: 'metrics'.
            ttl: Maximum metric lifetime in seconds.
        """
        self.data_type = data_type
        self.ttl = ttl
        self.wrapped = wrapped

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"<{self.__class__.__name__}:{self.data_type}:"
            f"wrapped={self.wrapped}:ttl={self.ttl}>"
        )


class CollectKeys:
    """
    This message initiates collection of record keys from a queue.

    Keys are being returned as a list of Tuples: (key: bytes, timestamp: int).
    """

    __slots__ = ["data_type", "wrapped", "amount", "reversed"]

    def __init__(self, data_type: str, wrapped: bool, amount: int = 0):
        """
        Args:
            data_type: Type of data to collect. E.g.: 'metrics'.
            wrapped: Whether a Storage should collect from a queue of
                     processed data.
            amount: Maximum number of keys that we want to collect. 0 is all.
        """
        self.amount = amount
        self.data_type = data_type
        self.wrapped = wrapped

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"<{self.__class__.__name__}:{self.data_type}:"
            f"wrapped={self.wrapped}:amount={self.amount}>"
        )


class CollectValues:
    """
    This message initiates collection of record values from a queue.

    Values are being returned as a list of bytes.
    """

    __slots__ = ["data_type", "keys", "wrapped"]

    def __init__(self, data_type: str, keys: List[bytes], wrapped: bool):
        """
        Args:
            data_type: Type of data to collect. E.g.: 'metrics'.
            wrapped: Whether a Storage should collect from a queue of
                     processed data.
            keys: List of keys of the records that we want to collect.
        """
        self.data_type = data_type
        self.keys = keys
        self.wrapped = wrapped

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"<{self.__class__.__name__}:{self.data_type}:"
            f"wrapped={self.wrapped}:keys_number={len(self.keys)}>"
        )


class DeleteRecords:
    """
    This message initiates deletion of record values from a queue.
    """

    __slots__ = ["data_type", "keys", "wrapped"]

    def __init__(self, data_type: str, keys: List[bytes], wrapped: bool):
        """
        Args:
            data_type: Type of data to delete. E.g.: 'metrics'.
            wrapped: Whether a Storage should delete from a queue of
                     processed data.
            keys: List of keys of the records that we want to delete.
        """
        self.data_type = data_type
        self.keys = keys
        self.wrapped = wrapped

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"<{self.__class__.__name__}:{self.data_type}:"
            f"wrapped={self.wrapped}:keys_number={len(self.keys)}>"
        )


class StoreRecords:
    """
    This message initiates deletion of record values in a queue.
    """

    __slots__ = ["data_type", "records", "wrapped"]

    def __init__(self, data_type: str, records: Iterable, wrapped: bool):
        """
        Args:
            data_type: Type of data to store. E.g.: 'metrics'.
            wrapped: Whether a Storage should store to a queue of
                     processed data.
            records: Iterable of preprocessed objects with `asdict` method.
                     E.g.: WrappedMetric.
        """
        self.data_type = data_type
        self.records = list(records)
        self.wrapped = wrapped

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return (
            f"<{self.__class__.__name__}:{self.data_type}:"
            f"wrapped={self.wrapped}:records_number={len(self.records)}>"
        )
