import asyncio
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from icecast_collector import process_audio_chunk

from backend.pipeline.ingestion import icecast_collector


class TestProcessAudioChunk(unittest.TestCase):
    def setUp(self) -> None:
        # 1. Setup Mocks
        self.mock_bucket = MagicMock()
        self.mock_logger = MagicMock()

        # 2. Setup Patchers for icecast_collector namespace
        self.patchers = [
            patch("icecast_collector.bucket", self.mock_bucket),
            patch("icecast_collector.logger", self.mock_logger),
            patch("icecast_collector.SAMPLE_WIDTH", icecast_collector.SAMPLE_WIDTH),
            patch("icecast_collector.SAMPLE_RATE", icecast_collector.SAMPLE_RATE),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self) -> None:
        for p in self.patchers:
            p.stop()

    @patch("icecast_collector.datetime")
    def test_process_audio_chunk_success(self, mock_datetime: MagicMock) -> None:
        """Test a successful WAV conversion and upload with exact path verification."""
        # Arrange: Fix the time so we can predict the filename exactly
        fixed_now = datetime(2026, 2, 27, 14, 30, 0)
        mock_datetime.now.return_value = fixed_now
        expected_time_str = "2026-02-27T14-30-00"  # ISO with : replaced by -

        fake_chunk = bytearray(b"\x00\x01" * 100)
        feed_id = 123
        source_type = "bcfy_feeds"
        expected_blob_name = f"bcfy_feeds/123/{expected_time_str}.wav"

        # Act
        process_audio_chunk(fake_chunk, feed_id, source_type)

        # Assert
        # Verify the blob was created with the correct path
        self.mock_bucket.blob.assert_called_once_with(expected_blob_name)

        # Verify upload content type
        mock_blob = self.mock_bucket.blob.return_value
        _, kwargs = mock_blob.upload_from_string.call_args
        self.assertEqual(kwargs["content_type"], "audio/wav")

        # Verify success was logged
        self.mock_logger.info.assert_called()

    def test_process_audio_chunk_wav_conversion_error(self) -> None:
        """Test handling of invalid raw data that breaks the wave module."""
        # Act: Pass something that isn't a buffer (None)
        process_audio_chunk(None, 123, "test")

        # Assert
        self.mock_logger.exception.assert_called()
        self.mock_bucket.blob.assert_not_called()

    def test_process_audio_chunk_gcs_error(self) -> None:
        """Test handling of GCS upload exceptions."""
        # Arrange: Make the upload throw an error
        mock_blob = MagicMock()
        self.mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_string.side_effect = Exception("Network Timeout")

        fake_chunk = bytearray(b"\x00\x01" * 10)

        # Act
        process_audio_chunk(fake_chunk, 123, "test")

        # Assert
        # Ensure the error was logged via logger.exception
        self.mock_logger.exception.assert_called()


class TestIcecastCollector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        # Mock constants/globals that your function relies on
        self.sample_rate_patch = patch("icecast_collector.SAMPLE_RATE", 16000)
        self.bytes_patch = patch("icecast_collector.BYTES_PER_CHUNK", 480000)
        self.throttle_patch = patch(
            "icecast_collector.STARTUP_THROTTLE", asyncio.Semaphore(1)
        )

        self.sample_rate_patch.start()
        self.bytes_patch.start()
        self.throttle_patch.start()

    def tearDown(self) -> None:
        patch.stopall()

    @patch(
        "icecast_collector.asyncio.to_thread", new_callable=AsyncMock
    )  # Mock the offloader
    @patch("icecast_collector.asyncio.create_subprocess_exec")
    @patch("icecast_collector.asyncio.sleep", new_callable=AsyncMock)
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

    @patch("icecast_collector.asyncio.create_subprocess_exec")
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
        with patch("icecast_collector.asyncio.sleep", side_effect=RuntimeError("Stop")):
            with self.assertRaises(RuntimeError):
                await icecast_collector.monitor_stream()

        # Verify cleanup logic
        mock_proc.terminate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
