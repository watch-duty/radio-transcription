import os
import unittest
from unittest.mock import MagicMock, patch

import fakeredis

with patch.dict(os.environ, {"REDIS_CERTIFICATE_PATH": "/secrets"}):
    from backend.common.storage.redis_service import RedisService


class TestRedisService(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_redis_factory = MagicMock(side_effect=fakeredis.FakeRedis)
        self.patcher = patch(
            "backend.common.storage.redis_service.Redis", self.mock_redis_factory
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

    def test_ssl_configuration_is_passed(self) -> None:
        self.mock_redis_factory.assert_called()

        _, kwargs = self.mock_redis_factory.call_args

        self.assertTrue(kwargs.get("ssl"), "SSL should be enabled")
        self.assertEqual(kwargs.get("ssl_cert_reqs"), "required")
        self.assertIn("/secrets/server_ca.pem", kwargs.get("ssl_ca_path", ""))

    @patch.dict(os.environ, {"LOCAL_DEV": "True"})
    def test_ssl_configuration_not_passed_in_local_dev(self) -> None:
        self.service = RedisService()

        self.mock_redis_factory.assert_called()

        _, kwargs = self.mock_redis_factory.call_args

        self.assertFalse(kwargs.get("ssl"), "SSL should not be enabled")
        self.assertEqual(kwargs.get("ssl_cert_reqs"), "none")
        self.assertIsNone(kwargs.get("ssl_ca_path"), "should be None")


if __name__ == "__main__":
    unittest.main()
