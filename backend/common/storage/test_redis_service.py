import unittest
from unittest.mock import patch

import fakeredis

from backend.common.storage.redis_service import RedisService


class TestRedisService(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher = patch(
            "backend.common.database.redis_service.Redis", fakeredis.FakeRedis
        )
        self.patcher.start()
        self.service = RedisService()

    def tearDown(self) -> None:
        self.patcher.stop()

    def test_set_success(self) -> None:
        key = "test-key"
        value = "test-value"
        ttl = 60

        result = self.service.set_if_not_exists(key, value, ttl)

        self.assertTrue(result)
        self.assertEqual(self.service.client.get(key), value)

    def test_set_already_exists(self) -> None:
        key = "existing-key"
        value = "initial-value"
        ttl = 60

        self.service.client.set(key, value)

        result = self.service.set_if_not_exists(key, "new-value", ttl)

        self.assertFalse(result)
        self.assertEqual(self.service.client.get(key), value)


if __name__ == "__main__":
    unittest.main()
