import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
    "GCS_BUCKET_NAME": "test-bucket",
}

with (
    patch("google.cloud.storage.Client"),
    patch.dict(os.environ, MOCK_ENV_VARS, clear=False),
):
    from backend.pipeline.ingestion import icecast_collector


class TestProcessAudioChunk(unittest.TestCase):
    def setUp(self) -> None:
        # 1. Setup Mocks
        self.mock_bucket = MagicMock()
        self.mock_logger = MagicMock()

        # 2. Setup Patchers for icecast_collector namespace
        self.patchers = [
            patch.object(icecast_collector, "bucket", self.mock_bucket),
            patch.object(icecast_collector, "logger", self.mock_logger),
            patch.object(
                icecast_collector, "SAMPLE_WIDTH", icecast_collector.SAMPLE_WIDTH
            ),
            patch.object(
                icecast_collector, "SAMPLE_RATE", icecast_collector.SAMPLE_RATE
            ),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self) -> None:
        for p in self.patchers:
            p.stop()

    @patch.object(icecast_collector, "datetime")
    def test_process_audio_chunk_success(self, mock_datetime: MagicMock) -> None:
        """Test a successful WAV conversion and upload with exact path verification."""
        # Arrange
        chunk = b"\x00\x01" * 1000
        feed_id = 1234
        source_type = "bcfy_feeds"
        mock_datetime.now.return_value.isoformat.return_value = (
            "2026-02-27T12:34:56.000000"
        )

        mock_blob = MagicMock()
        self.mock_bucket.blob.return_value = mock_blob

        # Act
        icecast_collector.process_audio_chunk(
            bytearray(chunk), feed_id, source_type, "2026-02-27T12-34-56.000000"
        )

        # Assert
        expected_blob_name = "bcfy_feeds/1234/2026-02-27T12-34-56.000000.wav"
        self.mock_bucket.blob.assert_called_once_with(expected_blob_name)
        mock_blob.upload_from_string.assert_called_once()
        upload_args, upload_kwargs = mock_blob.upload_from_string.call_args

        self.assertTrue(len(upload_args[0]) > 44)  # WAV header + PCM payload
        self.assertEqual(upload_kwargs["content_type"], "audio/wav")
        self.mock_logger.info.assert_called_once()
        self.mock_logger.exception.assert_not_called()

    def test_process_audio_chunk_wav_conversion_error(self) -> None:
        """Test handling of invalid raw data that breaks the wave module."""
        # Arrange
        chunk = b"bad-audio"
        feed_id = 5678
        source_type = "bcfy_feeds"

        with patch.object(
            icecast_collector.wave, "open", side_effect=Exception("wave failure")
        ):
            # Act
            icecast_collector.process_audio_chunk(
                bytearray(chunk), feed_id, source_type, "2026-02-27T12-34-56.000000"
            )

        # Assert
        self.mock_bucket.blob.assert_not_called()
        self.mock_logger.exception.assert_called_once()
        logged_msg = self.mock_logger.exception.call_args[0][0]
        self.assertIn("Failed to format WAV data", logged_msg)
        self.assertIn(str(feed_id), logged_msg)

    @patch.object(icecast_collector, "datetime")
    def test_process_audio_chunk_gcs_error(self, mock_datetime: MagicMock) -> None:
        """Test handling of GCS upload exceptions."""
        # Arrange
        chunk = b"\x00\x01" * 1000
        feed_id = 9012
        source_type = "bcfy_feeds"
        mock_datetime.now.return_value.isoformat.return_value = (
            "2026-02-27T23:59:59.999999"
        )

        mock_blob = MagicMock()
        mock_blob.upload_from_string.side_effect = Exception("gcs failure")
        self.mock_bucket.blob.return_value = mock_blob

        # Act
        icecast_collector.process_audio_chunk(
            bytearray(chunk), feed_id, source_type, "2026-02-27T23-59-59.999999"
        )

        # Assert
        expected_blob_name = "bcfy_feeds/9012/2026-02-27T23-59-59.999999.wav"
        self.mock_bucket.blob.assert_called_once_with(expected_blob_name)
        mock_blob.upload_from_string.assert_called_once()
        self.mock_logger.info.assert_not_called()
        self.mock_logger.exception.assert_called_once()
        logged_msg = self.mock_logger.exception.call_args[0][0]
        self.assertIn("Failed to upload", logged_msg)
        self.assertIn(expected_blob_name, logged_msg)


class TestIcecastCollector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bucket = MagicMock()
        self.mock_logger = MagicMock()
        self.patchers = [
            patch(
                "backend.pipeline.ingestion.icecast_collector.bucket", self.mock_bucket
            ),
            patch(
                "backend.pipeline.ingestion.icecast_collector.logger", self.mock_logger
            ),
            patch(
                "backend.pipeline.ingestion.icecast_collector.SAMPLE_WIDTH",
                icecast_collector.SAMPLE_WIDTH,
            ),
            patch(
                "backend.pipeline.ingestion.icecast_collector.SAMPLE_RATE",
                icecast_collector.SAMPLE_RATE,
            ),
            patch(
                "backend.pipeline.ingestion.icecast_collector.STARTUP_THROTTLE",
                asyncio.Semaphore(1),
            ),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self) -> None:
        for p in self.patchers:
            p.stop()

    @patch(
        "backend.pipeline.ingestion.icecast_collector.asyncio.to_thread",
        new_callable=AsyncMock,
    )  # Mock the offloader
    @patch(
        "backend.pipeline.ingestion.icecast_collector.asyncio.create_subprocess_exec"
    )
    @patch(
        "backend.pipeline.ingestion.icecast_collector.asyncio.sleep",
        new_callable=AsyncMock,
    )
    async def test_monitor_stream_processes_data_and_restarts(
        self,
        mock_sleep: MagicMock,
        mock_subprocess: MagicMock,
        mock_to_thread: MagicMock,
    ) -> None:
        """Test that monitor_stream reads from ffmpeg and offloads processing."""
        # 1. Setup Mock Process
        mock_proc = AsyncMock()
        mock_proc.pid = 1234
        mock_proc.returncode = 0

        # Simulate exactly one full chunk (480000 bytes) then EOF
        mock_proc.stdout.read.side_effect = [b"a" * 480000, b""]
        mock_subprocess.return_value = mock_proc

        # 2. Break the infinite loop on the first restart
        mock_sleep.side_effect = [None, RuntimeError("Break Loop")]

        # 3. Execution
        try:
            await icecast_collector.monitor_stream()
        except RuntimeError as e:
            if str(e) != "Break Loop":
                raise

        # 4. Assertions
        # Verify to_thread was called with (func, chunk, feed_id, type)
        mock_to_thread.assert_called_once()

        # Access the arguments of the first call to to_thread
        args, _ = mock_to_thread.call_args
        # args[0] is the function, args[1] is the data chunk
        self.assertEqual(len(args[1]), 480000)

        # Verify that create_subprocess_exec was called twice
        # (once for initial startup, once after the first iteration restart)
        self.assertEqual(mock_subprocess.call_count, 2)

    @patch(
        "backend.pipeline.ingestion.icecast_collector.asyncio.create_subprocess_exec"
    )
    async def test_ffmpeg_termination_on_failure(
        self, mock_subprocess: MagicMock
    ) -> None:
        """Ensure process.terminate() is called if the loop crashes."""
        mock_proc = AsyncMock()
        mock_proc.pid = 999
        mock_proc.returncode = None  # Process still "running"

        # Force an error immediately inside the try block
        mock_proc.stdout.read.side_effect = Exception("System Failure")
        mock_subprocess.return_value = mock_proc

        # Break the outer loop immediately
        with patch(
            "backend.pipeline.ingestion.icecast_collector.asyncio.sleep",
            side_effect=RuntimeError("Stop"),
        ):
            with self.assertRaises(RuntimeError):
                await icecast_collector.monitor_stream()

        # Verify cleanup logic
        mock_proc.terminate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
