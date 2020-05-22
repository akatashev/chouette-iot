class CollectKeys:
    __slots__ = ["data_type", "wrapped"]

    def __init__(self, data_type, wrapped=False):
        self.data_type = data_type
        self.wrapped = wrapped


class CollectValues:
    __slots__ = ["data_type", "keys", "wrapped"]

    def __init__(self, data_type, keys, wrapped=False):
        self.data_type = data_type
        self.keys = keys
        self.wrapped = wrapped


class DeleteRecords:
    __slots__ = ["data_type", "keys", "wrapped"]

    def __init__(self, data_type, keys, wrapped=False):
        self.data_type = data_type
        self.keys = keys
        self.wrapped = wrapped


class StoreMetrics:
    __slots__ = ["records"]

    def __init__(self, records):
        self.records = records
