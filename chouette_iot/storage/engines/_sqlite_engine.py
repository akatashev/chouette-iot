"""
Storage Engine for SQLite storage type.

This is an EXPERIMENTAL storage, therefore it has plenty of code duplication
and probably will be removed. It's STRONGLY advised to use Redis as a storage
since it was tested and proven its reliability.
"""

import json
import logging
import time
from typing import Any, List, Tuple, Union
from uuid import uuid4

import sqlite3

from pydantic import BaseSettings
from ._storage_engine import StorageEngine
from ..messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    GetQueueSize,
    StoreRecords,
)

__all__ = ["SQLiteEngine"]

logger = logging.getLogger("chouette-iot")


class SQLiteConfig(BaseSettings):
    """
    SQLite config reads a path to SQLite DB file that is used as a storage.
    """

    chouette_db_path: str = "/chouette/chouette.sqlite"


class SQLiteEngine(StorageEngine):
    """
    Storage engine for SQLite storage type.
    """

    def __init__(self):
        """
        Using 'check_same_thread=False' can be a problem in a non-thread safe
        application. Luckily, actor model means that an actor handles just
        a single message at a time, so the same SQLite connection won't be
        used simultaneously by multiple threads.

        If it can't connect to a DB, it raises a RuntimeException to stop
        the application by killing a vital actor, since while Redis is
        a service of its own, SQLite file must be owned by Chouette and if
        it can't create or read it, it's unlikely that it will be able to
        do this in the future without a manual interaction.
        """
        config = SQLiteConfig()
        self.db_path = config.chouette_db_path
        self.name = self.__class__.__name__
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.text_factory = bytes
            self._create_tables(self.conn)
        except sqlite3.Error as error:
            logger.critical(
                "[%s] Can't connect to db %s due to %s.", self.name, self.db_path, error
            )
            raise RuntimeError("Could not initialize SQLite storage.")

    def _create_tables(self, connection: sqlite3.Connection) -> bool:
        """
        Creates tables for logs and metrics. These tables basically
        use the same structure as Redis storage but without dividing
        it into 2 different instances.

        This operation is idempotent.

        Args:
             connection: SQLite connection object.
        Returns: Boolean showing whether tables exist.
        """
        pattern = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            key TEXT,
            timestamp REAL,
            content TEXT,
            PRIMARY KEY (key)
        );
        """
        cur = connection.cursor()
        try:
            for table in ["metrics_raw", "metrics_wrapped", "logs_wrapped"]:
                query = pattern.format(table_name=table)
                cur.execute(query)
            connection.commit()
        except sqlite3.Error as error:
            logger.error(
                "[%s] Could not create tables in db %s due to %s.",
                self.name,
                self.db_path,
                error,
            )
            connection.rollback()
            return False
        return True

    def stop(self):
        """
        Tries to release connections to SQLite database.
        """
        self.conn.close()

    def cleanup_outdated(self, request: CleanupOutdatedRecords) -> bool:
        """
        Cleans up outdated records in a specified queue.

        Datadog rejects metrics older than 4 hours (default TTL), so before
        trying to dispatch anything Chouette cleans up outdated metrics.

        Args:
            request: CleanupOutdated message with record type and TTL.
        Returns: Boolean that says whether execution was successful.
        """
        table_name = self._get_table_name(request)
        ttl = request.ttl
        query = f"DELETE * FROM {table_name} WHERE timestamp < ?;"
        cur = self.conn.cursor()
        cur.execute(query, str(time.time() - ttl))
        try:
            self.conn.commit()
        except sqlite3.Error as error:
            logger.warning(
                "[%s] Could not cleanup outdated records in a table '%s' due to: '%s'.",
                self.name,
                table_name,
                error,
            )
            self.conn.rollback()
            return False
        return True

    def collect_keys(self, request: CollectKeys) -> List[Tuple[bytes, int]]:
        """
        Tries to collect keys from a specified queue.

        CollectKeys message has the following properties:
        * data_type - type of a queue, e.g.: 'metrics'.
        * wrapped - whether that's a queue of processed records or not.
        * amount - how many keys should be collected. 0 means `all of them`.

        It returns a list of tuples with keys and their timestamps:
        (key: bytes, timestamp: int).

        Args:
            request: CollectKeys message.
        Returns: List of collected keys as tuples.
        """
        table_name = self._get_table_name(request)
        query = f"SELECT key, timestamp FROM {table_name} ORDER BY timestamp LIMIT ?;"
        cur = self.conn.cursor()
        try:
            cur.execute(query, str(request.amount))
            keys = cur.fetchall()
        except sqlite3.Error as error:
            logger.warning(
                "[%s] Could not collect keys from a table '%s' due to: '%s'.",
                self.name,
                table_name,
                error,
            )
            self.conn.rollback()
            return []
        logger.debug(
            "[%s] Collected %s keys from a table '%s'.",
            self.name,
            len(keys),
            table_name,
        )
        return keys

    def collect_values(self, request: CollectValues) -> List[bytes]:
        """
        Tries to collect values by keys from a specified queue.

        Receives a CollectValues message that contains a list of keys.

        Args:
            request: CollectValues message with specified keys.
        Returns: List of collected values.
        """
        table_name = self._get_table_name(request)
        if not request.keys:
            logger.debug(
                "[%s] No keys were specified to collect values for a table '%s'.",
                self.name,
                table_name,
            )
            return []
        query = f"SELECT content FROM {table_name} WHERE key IN (?);"
        cur = self.conn.cursor()
        try:
            cur.execute(query, request.keys)
            values = cur.fetchall()
        except sqlite3.Error as error:
            logger.warning(
                "[%s] Could not collect records from a table '%s' due to: '%s'.",
                self.name,
                table_name,
                error,
            )
            self.conn.rollback()
            return []
        logger.debug(
            "[%s] Collected %s records from a table '%s'.",
            self.name,
            len(values),
            table_name,
        )
        return values

    def delete_records(self, request: DeleteRecords) -> bool:
        """
        Tries to delete records with specified keys.

        Args:
            request: DeleteRecords message with specified keys.
        Returns: Boolean that says whether execution was successful.
        """
        table_name = self._get_table_name(request)
        if not request.keys:
            logger.debug(
                "[%s] Nothing to delete from a table '%s'.", self.name, table_name
            )
            return True
        query = f"DELETE * FROM {table_name} WHERE key IN (?);"
        cur = self.conn.cursor()
        try:
            cur.execute(query, request.keys)
            self.conn.commit()
        except sqlite3.Error as error:
            logger.warning(
                "[%s] Could not remove %s records from a table '%s' due to: '%s'.",
                self.name,
                len(request.keys),
                table_name,
                error,
            )
            self.conn.rollback()
            return False
        logger.debug(
            "[%s] Deleted %s records from a table '%s'.",
            self.name,
            len(request.keys),
            table_name,
        )
        return True

    def get_queue_size(self, request: GetQueueSize) -> int:
        """
        Tried to get a size of a specified queue.

        In case of error returns -1, because values less than 1 SHOULD be
        filtered.

        Args:
            request: GetQueueSize message.
        Returns: Size of a specified queue.
        """
        table_name = self._get_table_name(request)
        query = f"SELECT COUNT(*) FROM {table_name};"
        cur = self.conn.cursor()
        try:
            cur.execute(query)
            table_size = cur.fetchall().pop()
        except sqlite3.Error as error:
            logger.warning(
                "[%s] Could not calculate %s table size due to: '%s'.",
                self.name,
                table_name,
                error,
            )
            self.conn.rollback()
            return -1
        return table_size

    def store_records(self, request: StoreRecords) -> bool:
        """
        Tries to store received records to a queue.

        It automatically generates a unique id for every record and stores
        its content to a table.

        If it can't cast one of the records to a dict via `asdict()` method,
        it ignores this record and tries to store all other records.

        Args:
            request: StoreRecords with an iterable of suitable objects.
        Returns: Boolean that says whether execution was successful.
        """
        table_name = self._get_table_name(request)
        records_list = list(request.records)
        values = []
        for record in records_list:
            try:
                record_value = json.dumps(record.asdict())
            except AttributeError:
                continue
            record_key = str(uuid4())
            values.append((record_key, record.timestamp, record_value))
        stored_metrics = len(values)
        if not values:
            logger.debug(
                "[%s] Nothing to store to a table '%s'.", self.name, table_name
            )
            return True
        query = f"INSERT INTO {table_name} VALUES (?, ?, ?);"
        cur = self.conn.cursor()
        try:
            cur.executemany(query, values)
            self.conn.commit()
        except sqlite3.Error as error:
            logger.warning(
                "[%s] Could not store %s/%s records to a table '%s' due to: '%s'.",
                self.name,
                stored_metrics,
                len(records_list),
                table_name,
                error,
            )
            self.conn.rollback()
            return False
        logger.debug(
            "[%s] Stored %s/%s records to a table '%s'.",
            self.name,
            stored_metrics,
            len(records_list),
            table_name,
        )
        return True

    @staticmethod
    def _get_table_name(request: Any) -> str:
        """
        Takes a request and returns a table name.

        Args:
            request: One of `chouette.storage.messages` objects.
        Return: String with a table name.
        """
        record_type = "wrapped" if request.wrapped else "raw"
        table_name = f"{request.data_type}_{record_type}"
        return table_name
