from backend.common.storage.cache_provider import CacheProvider


class MockCacheProvider(CacheProvider):
    """
    Mock implementation of a CacheProvider for use in testing.
    """

    def __init__(self) -> None:
        self.cache = {}

    def set_if_not_exists(self, key: str, value: str, ttl: int) -> bool:
        if key in self.cache:
            return True
        self.cache[key] = value
        return False

    def get_value(self, key: str) -> str | None:
        if key in self.cache:
            return self.cache[key]
        return None
