from typing import Iterable, List

__all__ = [
    "CleanupOutdatedRecords",
    "CollectKeys",
    "CollectValues",
    "DeleteRecords",
    "StoreRecords",
]


class CleanupOutdatedRecords:
    __slots__ = ["data_type", "ttl"]

    def __init__(self, data_type: str, ttl: int):
        self.data_type = data_type
        self.ttl = ttl


class CollectKeys:
    __slots__ = ["data_type", "wrapped", "amount", "reversed"]

    def __init__(self, data_type: str, amount: int = 0, wrapped: bool = False):
        self.amount = amount
        self.data_type = data_type
        self.wrapped = wrapped


class CollectValues:
    __slots__ = ["data_type", "keys", "wrapped"]

    def __init__(self, data_type: str, keys: List[bytes], wrapped: bool = False):
        self.data_type = data_type
        self.keys = keys
        self.wrapped = wrapped


class DeleteRecords:
    __slots__ = ["data_type", "keys", "wrapped"]

    def __init__(self, data_type: str, keys: List[bytes], wrapped: bool = False):
        self.data_type = data_type
        self.keys = keys
        self.wrapped = wrapped


class StoreRecords:
    __slots__ = ["data_type", "records", "wrapped"]

    def __init__(self, data_type: str, records: Iterable, wrapped: bool = False):
        self.records = records
        self.data_type = data_type
        self.wrapped = wrapped
