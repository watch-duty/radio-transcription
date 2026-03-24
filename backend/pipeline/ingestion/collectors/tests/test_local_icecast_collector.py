import datetime
import os
import tempfile
import unittest
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

from anyio import Path as AsyncPath

MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
}


with (
    patch.dict(os.environ, MOCK_ENV_VARS, clear=False),
):
    from backend.pipeline.ingestion.collectors import local_icecast_collector


class TestLocalIcecastCollector(unittest.IsolatedAsyncioTestCase):
    """Tests for local_icecast_collector local debugging entrypoint."""

    async def test_run_local_capture_requires_stream_url_env_var(self) -> None:
        """Raises ValueError when ICECAST_STREAM_URL is unset."""
        with patch.dict(
            os.environ,
            {
                "ICECAST_STREAM_URL": "",
                "ICECAST_LOCAL_OUTPUT_DIR": "",
            },
            clear=False,
        ):
            with self.assertRaises(ValueError) as context:
                await local_icecast_collector.run_local_capture()

        self.assertIn("ICECAST_STREAM_URL must be set", str(context.exception))

    async def test_run_local_capture_writes_bytes_and_calls_capture(
        self,
    ) -> None:
        """Writes captured bytes to disk and calls capture_icecast_stream once."""

        async def _fake_capture(_feed: dict[str, Any], _shutdown_event: Any):
            yield b"first-bytes", datetime.datetime.now()
            yield b"second-bytes", datetime.datetime.now()

        fixed_feed_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        capture_mock = MagicMock(side_effect=_fake_capture)

        with tempfile.TemporaryDirectory() as tmp_dir:
            with (
                patch.dict(
                    os.environ,
                    {
                        "ICECAST_STREAM_URL": "http://example.com/stream",
                        "ICECAST_LOCAL_OUTPUT_DIR": tmp_dir,
                    },
                    clear=False,
                ),
                patch.object(
                    local_icecast_collector,
                    "capture_icecast_stream",
                    capture_mock,
                ),
                patch.object(
                    local_icecast_collector.uuid,
                    "uuid4",
                    return_value=fixed_feed_id,
                ),
            ):
                await local_icecast_collector.run_local_capture()

            # Ensure capture function was called with expected feed metadata.
            capture_mock.assert_called_once()
            feed_arg, shutdown_event_arg = capture_mock.call_args.args
            self.assertEqual(feed_arg["id"], fixed_feed_id)
            self.assertEqual(feed_arg["name"], "local-icecast-test")
            self.assertEqual(feed_arg["source_type"], "icecast")
            self.assertIsNone(feed_arg["last_processed_filename"])
            self.assertEqual(
                feed_arg["stream_url"], "http://example.com/stream"
            )
            self.assertIsInstance(
                shutdown_event_arg, local_icecast_collector.asyncio.Event
            )

            # Ensure two chunk files were written and include expected bytes.
            written_files = sorted(
                [path async for path in AsyncPath(tmp_dir).glob("chunk_*.flac")]
            )
            self.assertEqual(len(written_files), 2)
            self.assertEqual(
                await written_files[0].read_bytes(), b"first-bytes"
            )
            self.assertEqual(
                await written_files[1].read_bytes(), b"second-bytes"
            )

    async def test_run_local_capture_uses_cwd_when_output_dir_not_set(
        self,
    ) -> None:
        """Falls back to current working directory when output dir env var is unset."""

        async def _fake_capture(_feed: dict[str, Any], _shutdown_event: Any):
            yield b"cwd-bytes", datetime.datetime.now()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = local_icecast_collector.Path(tmp_dir)
            with (
                patch.dict(
                    os.environ,
                    {
                        "ICECAST_STREAM_URL": "http://example.com/stream",
                        "ICECAST_LOCAL_OUTPUT_DIR": "",
                    },
                    clear=False,
                ),
                patch.object(
                    local_icecast_collector.Path, "cwd", return_value=tmp_path
                ),
                patch.object(
                    local_icecast_collector,
                    "capture_icecast_stream",
                    MagicMock(side_effect=_fake_capture),
                ),
            ):
                await local_icecast_collector.run_local_capture()

            written_files = [
                path async for path in AsyncPath(tmp_dir).glob("chunk_*.flac")
            ]
            self.assertEqual(len(written_files), 1)
            self.assertEqual(await written_files[0].read_bytes(), b"cwd-bytes")


if __name__ == "__main__":
    unittest.main()
