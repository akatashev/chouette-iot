"""
Interface definition for all the Storage Engine implementations.
"""
from abc import ABC, abstractmethod
from typing import List, Tuple

from ..messages import (
    CleanupOutdatedRecords,
    CollectKeys,
    CollectValues,
    DeleteRecords,
    GetQueueSize,
    StoreRecords,
)

__all__ = ["StorageEngine"]


class StorageEngine(ABC):
    """
    Interface for all Storage Engine implementations.
    """

    @abstractmethod
    def stop(self):
        """
        Stops a storage.
        """
        raise NotImplementedError(
            "Use a concrete StorageEngine class."
        )  # pragma: no cover

    @abstractmethod
    def cleanup_outdated(self, request: CleanupOutdatedRecords) -> bool:
        """
        Cleans up outdated records in a specified queue.
        """
        raise NotImplementedError(
            "Use a concrete StorageEngine class."
        )  # pragma: no cover

    @abstractmethod
    def collect_keys(self, request: CollectKeys) -> List[Tuple[bytes, int]]:
        """
        Tries to collect keys from a specified queue.
        """
        raise NotImplementedError(
            "Use a concrete StorageEngine class."
        )  # pragma: no cover

    @abstractmethod
    def collect_values(self, request: CollectValues) -> List[bytes]:
        """
        Tries to collect values by keys from a specified queue.
        """
        raise NotImplementedError(
            "Use a concrete StorageEngine class."
        )  # pragma: no cover

    @abstractmethod
    def delete_records(self, request: DeleteRecords) -> bool:
        """
        Tries to delete records with specified keys.
        """
        raise NotImplementedError(
            "Use a concrete StorageEngine class."
        )  # pragma: no cover

    @abstractmethod
    def get_queue_size(self, request: GetQueueSize) -> int:
        """
        Tried to get a size of a specified queue.
        """
        raise NotImplementedError(
            "Use a concrete StorageEngine class."
        )  # pragma: no cover

    @abstractmethod
    def store_records(self, request: StoreRecords) -> bool:
        """
        Tries to store received records to a queue.
        """
        raise NotImplementedError(
            "Use a concrete StorageEngine class."
        )  # pragma: no cover
