from typing import Iterable, List


class CleanupOutdated:
    __slots__ = ["data_type", "metric_ttl"]

    def __init__(self, data_type: str, metric_ttl: int):
        self.data_type = data_type
        self.metric_ttl = metric_ttl


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


class StoreMetrics:
    __slots__ = ["records"]

    def __init__(self, records: Iterable):
        self.records = records
