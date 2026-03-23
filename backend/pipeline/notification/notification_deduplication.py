import time

from backend.pipeline.common.storage.cache_provider import CacheProvider

TTL_IN_SECONDS = 3600  # One hour


class NotificationDeduplication:
    """
    Dedupes notifications. Provides a method for setting the notification ID in the provided cache.
    """

    def __init__(self, cache: CacheProvider) -> None:
        self.cache = cache

    def process_notification(self, notification_id: str) -> bool:
        """
        Processes the notification_id by setting the ID as a key in the cache, if not does not exist.
        Sets the value to current time. Value is optional, but using the time can help with debugging.
        """
        now = str(time.time())
        return self.cache.set_if_not_exists(
            notification_id, now, TTL_IN_SECONDS
        )
