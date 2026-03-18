from abc import ABC, abstractmethod


class CacheProvider(ABC):
    """
    Abstract method class for setting a value in a cache.
    """

    @abstractmethod
    def set_if_not_exists(self, key: str, value: str, ttl: int) -> bool:
        """
        Sets the key/value pair in the cache for the TTL (seconds), given that the key does not already exist.
        Returns True if it does exist, or False if not.
        """
