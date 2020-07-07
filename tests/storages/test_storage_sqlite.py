from chouette_iot.storage import StorageActor
from pykka import ActorRegistry
import pytest
import json
import sqlite3
from chouette_iot.metrics._metrics import WrappedMetric
from chouette_iot.storage.messages import StoreRecords
from unittest.mock import MagicMock


@pytest.fixture
def storage_actor_sqlite(monkeypatch):
    """
    SQLite actor fixture.
    """
    ActorRegistry.stop_all()
    monkeypatch.setenv("API_KEY", "whatever")
    monkeypatch.setenv("GLOBAL_TAGS", '["chouette-iot:est:chouette-iot"]')
    monkeypatch.setenv("METRICS_WRAPPER", "simple")
    monkeypatch.setenv("CHOUETTE_STORAGE_TYPE", "sqlite")
    actor_ref = StorageActor.get_instance()
    yield actor_ref
    actor_ref.stop()


@pytest.fixture(scope="module")
def db_connection():
    conn = sqlite3.connect("/chouette/chouette.sqlite")
    conn.text_factory = bytes
    yield conn
    conn.close()


def test_sqlite_store_records(storage_actor_sqlite, db_connection):
    records = [
        WrappedMetric(metric="test", type="count", value=1),
        WrappedMetric(metric="test2", type="gauge", value=2),
    ]
    storage_actor_sqlite.ask(StoreRecords("metrics", records, wrapped=True))
    c = db_connection.cursor()
    c.execute("SELECT * FROM metrics_wrapped;")
    db_content = c.fetchall()
    assert len(db_content) == len(records)
    stored_records = set(content.decode() for key, ts, content in db_content)
    assert stored_records == set(json.dumps(record.asdict()) for record in records)
