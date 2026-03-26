import unittest
import uuid
from unittest.mock import patch

from backend.pipeline.ingestion.settings import NormalizerSettings


def _required_env() -> dict[str, str]:
    return {
        "COLLECTOR_OUTPUT_BUCKET": "staging-bucket",
        "PUBSUB_TOPIC_PATH": "projects/test-project/topics/test-topic",
        "ALLOYDB_HOST": "127.0.0.1",
        "ALLOYDB_USER": "radio_user",
        "ALLOYDB_DB": "radio_db",
    }


class TestNormalizerSettings(unittest.TestCase):
    """Test suite for environment-driven NormalizerSettings parsing."""

    def test_normal_expected_inputs(self) -> None:
        """Loads all settings from valid environment variables."""
        env = {
            **_required_env(),
            "WORKER_ID": "00000000-0000-0000-0000-000000000123",
            "MAX_FEEDS_PER_WORKER": "500",
            "LEASE_POLL_INTERVAL_SEC": "2.5",
            "HEARTBEAT_INTERVAL_SEC": "10.0",
            "HEARTBEAT_STALL_TIMEOUT_SEC": "30.0",
            "GRACEFUL_SHUTDOWN_TIMEOUT_SEC": "8.0",
            "ALLOYDB_POOL_MIN_SIZE": "3",
            "ALLOYDB_POOL_MAX_SIZE": "25",
            "ALLOYDB_COMMAND_TIMEOUT_SEC": "40.0",
            "ALLOYDB_CONNECT_TIMEOUT_SEC": "12.5",
            "FEED_FAILURE_THRESHOLD": "7",
            "ABANDONMENT_WINDOW_SEC": "120.0",
            "ALLOYDB_PORT": "6543",
            "ALLOYDB_PASSWORD": "secret",
            "GCS_UPLOAD_MAX_RETRIES": "5",
            "GCS_UPLOAD_RETRY_BASE_DELAY_SEC": "1.0",
            "GCS_UPLOAD_RETRY_MAX_DELAY_SEC": "16.0",
            "BOOKMARK_MAX_RETRIES": "4",
            "BOOKMARK_RETRY_BASE_DELAY_SEC": "0.25",
            "BOOKMARK_RETRY_MAX_DELAY_SEC": "2.0",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = NormalizerSettings()

        self.assertEqual(settings.worker_id, uuid.UUID(env["WORKER_ID"]))
        self.assertEqual(settings.max_feeds_per_worker, 500)
        self.assertEqual(settings.lease_poll_interval_sec, 2.5)
        self.assertEqual(settings.heartbeat_interval_sec, 10.0)
        self.assertEqual(settings.heartbeat_stall_timeout_sec, 30.0)
        self.assertEqual(settings.graceful_shutdown_timeout_sec, 8.0)
        self.assertEqual(settings.collector_output_bucket, "staging-bucket")
        self.assertEqual(settings.db.pool_min_size, 3)
        self.assertEqual(settings.db.pool_max_size, 25)
        self.assertEqual(settings.db.command_timeout_sec, 40.0)
        self.assertEqual(settings.db.connect_timeout_sec, 12.5)
        self.assertEqual(settings.feed_failure_threshold, 7)
        self.assertEqual(settings.abandonment_window_sec, 120.0)
        self.assertEqual(settings.db.host, "127.0.0.1")
        self.assertEqual(settings.db.port, 6543)
        self.assertEqual(settings.db.user, "radio_user")
        self.assertEqual(settings.db.db_name, "radio_db")
        self.assertEqual(settings.db.password, "secret")
        self.assertEqual(settings.gcs_upload_max_retries, 5)
        self.assertEqual(settings.gcs_upload_retry_base_delay_sec, 1.0)
        self.assertEqual(settings.gcs_upload_retry_max_delay_sec, 16.0)
        self.assertEqual(settings.bookmark_max_retries, 4)
        self.assertEqual(settings.bookmark_retry_base_delay_sec, 0.25)
        self.assertEqual(settings.bookmark_retry_max_delay_sec, 2.0)

    def test_edge_case_uses_defaults_and_generates_worker_id(self) -> None:
        """Uses defaults for optional settings when only required vars are set."""
        with patch.dict("os.environ", _required_env(), clear=True):
            settings = NormalizerSettings()

        self.assertIsInstance(settings.worker_id, uuid.UUID)
        self.assertEqual(settings.max_feeds_per_worker, 250)
        self.assertEqual(settings.lease_poll_interval_sec, 5.0)
        self.assertEqual(settings.heartbeat_interval_sec, 15.0)
        self.assertEqual(settings.heartbeat_stall_timeout_sec, 45.0)
        self.assertEqual(settings.graceful_shutdown_timeout_sec, 10.0)
        self.assertEqual(settings.db.pool_min_size, 5)
        self.assertEqual(settings.db.pool_max_size, 5)
        self.assertEqual(settings.db.command_timeout_sec, 30.0)
        self.assertEqual(settings.db.connect_timeout_sec, 10.0)
        self.assertEqual(settings.feed_failure_threshold, 5)
        self.assertEqual(settings.abandonment_window_sec, 60.0)
        self.assertEqual(settings.db.port, 6432)
        self.assertEqual(settings.db.password, "")
        self.assertEqual(settings.gcs_upload_max_retries, 3)
        self.assertEqual(settings.gcs_upload_retry_base_delay_sec, 0.5)
        self.assertEqual(settings.gcs_upload_retry_max_delay_sec, 8.0)
        self.assertEqual(settings.bookmark_max_retries, 2)
        self.assertEqual(settings.bookmark_retry_base_delay_sec, 0.5)
        self.assertEqual(settings.bookmark_retry_max_delay_sec, 4.0)

    def test_edge_case_zero_and_negative_numeric_values_parse(self) -> None:
        """Allows zero/negative values because parsing does not enforce ranges."""
        env = {
            **_required_env(),
            "MAX_FEEDS_PER_WORKER": "0",
            "LEASE_POLL_INTERVAL_SEC": "0.0",
            "HEARTBEAT_INTERVAL_SEC": "-1.0",
            "ALLOYDB_POOL_MIN_SIZE": "0",
            "ALLOYDB_POOL_MAX_SIZE": "-2",
            "ABANDONMENT_WINDOW_SEC": "-0.5",
        }

        with patch.dict("os.environ", env, clear=True):
            settings = NormalizerSettings()

        self.assertEqual(settings.max_feeds_per_worker, 0)
        self.assertEqual(settings.lease_poll_interval_sec, 0.0)
        self.assertEqual(settings.heartbeat_interval_sec, -1.0)
        self.assertEqual(settings.db.pool_min_size, 0)
        self.assertEqual(settings.db.pool_max_size, -2)
        self.assertEqual(settings.abandonment_window_sec, -0.5)

    def test_invalid_missing_required_env_var_raises(self) -> None:
        """Raises ValueError when a required environment variable is missing."""
        env = _required_env()
        del env["COLLECTOR_OUTPUT_BUCKET"]

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError) as context:
                NormalizerSettings()

        self.assertIn("COLLECTOR_OUTPUT_BUCKET", str(context.exception))

    def test_invalid_empty_required_env_var_raises(self) -> None:
        """Raises ValueError when a required environment variable is empty."""
        env = {**_required_env(), "COLLECTOR_OUTPUT_BUCKET": ""}

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError) as context:
                NormalizerSettings()

        self.assertIn("COLLECTOR_OUTPUT_BUCKET", str(context.exception))

    def test_invalid_worker_id_raises(self) -> None:
        """Raises ValueError when WORKER_ID is not a valid UUID."""
        env = {**_required_env(), "WORKER_ID": "not-a-uuid"}

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                NormalizerSettings()

    def test_invalid_integer_env_raises(self) -> None:
        """Raises ValueError for non-integer integer-backed settings."""
        env = {**_required_env(), "MAX_FEEDS_PER_WORKER": "abc"}

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                NormalizerSettings()

    def test_invalid_float_env_raises(self) -> None:
        """Raises ValueError for non-float float-backed settings."""
        env = {**_required_env(), "LEASE_POLL_INTERVAL_SEC": "not-a-float"}

        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(ValueError):
                NormalizerSettings()


if __name__ == "__main__":
    unittest.main()
