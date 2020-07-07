"""
Storage Engines Factory
"""
# pylint: disable=too-few-public-methods
from typing import Dict, Type

from ._redis_engine import RedisEngine
from ._sqlite_engine import SQLiteEngine
from ._storage_engine import StorageEngine

__all__ = ["EnginesFactory"]


class EnginesFactory:
    """
    Storage Engines factory tries to generate a specified storage engine.
    """

    storage_classes: Dict[str, Type[StorageEngine]] = {
        "redis": RedisEngine,
        "sqlite": SQLiteEngine,
    }

    @classmethod
    def get_engine(cls, storage_type: str) -> StorageEngine:
        """
        Factory method that takes a storage type string and returns a
        StorageEngine object.

        Default engine is RedisEngine.

        Args:
            storage_type: Engine type string.
        Returns: StorageEngine object.
        """
        engine_class = cls.storage_classes.get(storage_type.lower(), RedisEngine)
        engine_instance = engine_class()
        return engine_instance
