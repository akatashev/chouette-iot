from chouette.storages.messages import (
    CleanupOutdatedRecords,
    CollectValues,
    CollectKeys,
    DeleteRecords,
    StoreRecords,
)


def test_cleanup_str_and_repr():
    """
    CleanupOutdatedRecords:
    __str__ and __repr__  methods return the same string.
    """
    msg = CleanupOutdatedRecords("degrees", 451)
    assert str(msg) == f"<CleanupOutdatedRecords:degrees:ttl=451>"
    assert repr(msg) == str(msg)


def test_collect_keys_str_and_repr():
    """
    CollectKeys:
    __str__ and __repr__  methods return the same string.
    """
    msg = CollectKeys("politicians", amount=1984, wrapped=False)
    assert str(msg) == f"<CollectKeys:politicians:wrapped=False:amount=1984>"
    assert repr(msg) == str(msg)


def test_collect_values_str_and_repr():
    """
    CollectValues:
    __str__ and __repr__  methods return the same string.
    """
    msg = CollectValues("sweets", keys=[b"1", b"2", b"2"], wrapped=False)
    assert str(msg) == f"<CollectValues:sweets:wrapped=False:keys_number=3>"
    assert repr(msg) == str(msg)


def test_delete_records_str_and_repr():
    """
    DeleteRecords:
    __str__ and __repr__  methods return the same string.
    """
    msg = DeleteRecords("logs", keys=[b"3", b"2"], wrapped=False)
    assert str(msg) == f"<DeleteRecords:logs:wrapped=False:keys_number=2>"
    assert repr(msg) == str(msg)


def test_store_records_str_and_repr():
    """
    StoreRecords:
    __str__ and __repr__  methods return the same string.
    """
    msg = StoreRecords("logs", records=iter([]), wrapped=False)
    assert str(msg) == f"<StoreRecords:logs:wrapped=False:records_number=0>"
    assert repr(msg) == str(msg)
