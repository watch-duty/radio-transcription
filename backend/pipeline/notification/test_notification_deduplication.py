from unittest import TestCase, main, mock

from backend.common.storage.mock_cache_provider import MockCacheProvider
from backend.pipeline.notification.notification_deduplication import (
    NotificationDeduplication,
)


class TestNotificationDeduplication(TestCase):
    @mock.patch("time.time", return_value=12345)
    def test_has_duplicate(self, mock_time: mock.Mock) -> None:
        cache_provider = MockCacheProvider()
        notification_deduplication = NotificationDeduplication(cache_provider)
        self.assertFalse(notification_deduplication.process_notification("1234"))
        self.assertTrue(notification_deduplication.process_notification("1234"))

    @mock.patch("time.time", return_value=54321)
    def test_no_duplicate(self, mock_time: mock.Mock) -> None:
        cache_provider = MockCacheProvider()
        notification_deduplication = NotificationDeduplication(cache_provider)
        self.assertFalse(notification_deduplication.process_notification("1234"))
        self.assertEqual(cache_provider.get_value("1234"), "12345")


if __name__ == "__main__":
    main()
