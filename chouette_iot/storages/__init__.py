"""
chouette.storages
"""
# pylint: disable=too-few-public-methods
from typing import Dict, Type

from pykka import ActorRef  # type: ignore

from chouette_iot._singleton_actor import SingletonActor
from ._redis import RedisStorage

__all__ = ["StoragesFactory", "RedisStorage"]


class StoragesFactory:
    """
    Storages factory generates storages.
    """

    storages: Dict[str, Type[SingletonActor]] = {"redis": RedisStorage}

    @classmethod
    def get_storage(cls, storage_type: str) -> ActorRef:
        """
        Gets a storage type name and generates a storage actor, whose ActorRef
        is returned.
        RedisStorage is the default value and will be returned even for an
        incorrect storage_type.
        """
        storage_class = cls.storages.get(storage_type.lower(), RedisStorage)
        return storage_class.get_instance()
