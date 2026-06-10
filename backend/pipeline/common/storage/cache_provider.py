from abc import ABC, abstractmethod


class CacheProvider(ABC):
    """
    Abstract method class for setting a value in a cache.
    """

    @abstractmethod
    def set_if_not_exists(self, key: str, value: str, ttl: int) -> bool:
        """
        Sets the key/value pair in the cache for the TTL (seconds), given that the key does not already exist.
        Returns True if the key does not exist, or False if does.
        """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """
        Deletes the key from the cache.
        Returns True if the key existed and was deleted, False otherwise.
        """
