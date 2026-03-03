import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, call, patch

MOCK_ENV_VARS = {
    "BROADCASTIFY_USERNAME": "test_user",
    "BROADCASTIFY_PASSWORD": "test_pass",
    "FINAL_STAGING_BUCKET": "test-bucket",
}

with (
    patch("google.cloud.storage.Client"),
    patch.dict(os.environ, MOCK_ENV_VARS, clear=False),
):
    from backend.pipeline.ingestion import icecast_collector


class TestWriteToGCS(unittest.TestCase):
    def setUp(self) -> None:
        # 1. Setup Mocks
        self.mock_storage_client = MagicMock()
        self.mock_logger = MagicMock()

        # 2. Setup Patchers for icecast_collector namespace
        self.patchers = [
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
    def test_write_to_gcs_success(self, mock_datetime: MagicMock) -> None:
        """Test a successful WAV conversion and upload with exact path verification."""
        # Arrange
        chunk = b"\x00\x01" * 1000
        feed_id = 1234
        source_type = "bcfy_feeds"
        mock_datetime.now.return_value.isoformat.return_value = (
            "2026-02-27T12:34:56.000000"
        )

        # Make the mock storage client's upload method awaitable
        self.mock_storage_client.upload = AsyncMock()

        # Act
        asyncio.run(
            icecast_collector.write_to_gcs(
                self.mock_storage_client,
                bytearray(chunk),
                feed_id,
                source_type,
                "2026-02-27T12-34-56.000000",
            )
        )

        # Assert
        expected_blob_name = "bcfy_feeds/1234/2026-02-27T12-34-56.000000.wav"
        self.mock_storage_client.upload.assert_called_once()
        call_args, call_kwargs = self.mock_storage_client.upload.call_args

        self.assertEqual(call_args[0], icecast_collector.FINAL_STAGING_BUCKET)
        self.assertEqual(call_args[1], expected_blob_name)
        self.assertTrue(len(call_args[2]) > 44)  # WAV header + PCM payload
        self.assertEqual(call_kwargs["content_type"], "audio/wav")
        self.mock_logger.info.assert_called_once()
        self.mock_logger.exception.assert_not_called()

    def test_write_to_gcs_wav_conversion_error(self) -> None:
        """Test handling of invalid raw data that breaks the wave module."""
        # Arrange
        chunk = b"bad-audio"
        feed_id = 5678
        source_type = "bcfy_feeds"

        self.mock_storage_client.upload = AsyncMock()

        with patch.object(
            icecast_collector.wave, "open", side_effect=Exception("wave failure")
        ):
            # Act
            asyncio.run(
                icecast_collector.write_to_gcs(
                    self.mock_storage_client,
                    bytearray(chunk),
                    feed_id,
                    source_type,
                    "2026-02-27T12-34-56.000000",
                )
            )

        # Assert
        self.mock_storage_client.upload.assert_not_called()
        self.mock_logger.exception.assert_called_once()
        logged_msg = self.mock_logger.exception.call_args[0][0]
        self.assertIn("Failed to format WAV data", logged_msg)
        self.assertIn(str(feed_id), logged_msg)

    @patch.object(icecast_collector, "datetime")
    def test_write_to_gcs_gcs_error(self, mock_datetime: MagicMock) -> None:
        """Test handling of GCS upload exceptions."""
        # Arrange
        chunk = b"\x00\x01" * 1000
        feed_id = 9012
        source_type = "bcfy_feeds"
        mock_datetime.now.return_value.isoformat.return_value = (
            "2026-02-27T23:59:59.999999"
        )

        self.mock_storage_client.upload = AsyncMock(
            side_effect=Exception("gcs failure")
        )

        # Act
        asyncio.run(
            icecast_collector.write_to_gcs(
                self.mock_storage_client,
                bytearray(chunk),
                feed_id,
                source_type,
                "2026-02-27T23-59-59.999999",
            )
        )

        # Assert
        expected_blob_name = "bcfy_feeds/9012/2026-02-27T23-59-59.999999.wav"
        self.mock_storage_client.upload.assert_called_once()
        self.mock_logger.info.assert_not_called()
        self.mock_logger.exception.assert_called_once()
        logged_msg = self.mock_logger.exception.call_args[0][0]
        self.assertIn("Failed to upload", logged_msg)
        self.assertIn(expected_blob_name, logged_msg)


class TestIcecastCollector(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_storage_client = MagicMock()
        self.mock_logger = MagicMock()
        self.patchers = [
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
            await icecast_collector.monitor_stream(self.mock_storage_client)
        except RuntimeError as e:
            if str(e) != "Break Loop":
                raise

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
                await icecast_collector.monitor_stream(self.mock_storage_client)

        # Verify cleanup logic
        mock_proc.terminate.assert_called_once()

    @patch(
        "backend.pipeline.ingestion.icecast_collector.asyncio.sleep",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.icecast_collector._cleanup_stream",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.icecast_collector._process_stream_chunks",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_monitor_stream_exponential_backoff_consecutive_failures(
        self,
        mock_create_ffmpeg: AsyncMock,
        mock_process_chunks: AsyncMock,
        mock_cleanup: AsyncMock,
        mock_sleep: AsyncMock,
    ) -> None:
        """Backoff should grow exponentially for consecutive zero-chunk failures."""
        mock_proc = MagicMock()
        mock_proc.pid = 321
        mock_proc.returncode = 1
        mock_create_ffmpeg.return_value = mock_proc

        # Three failed iterations (no chunks), then stop.
        mock_process_chunks.side_effect = [0, 0, 0]
        mock_sleep.side_effect = [None, None, RuntimeError("Stop")]

        with self.assertRaises(RuntimeError):
            await icecast_collector.monitor_stream(self.mock_storage_client)

        self.assertEqual(
            mock_sleep.await_args_list,
            [
                call(10),
                call(20),
                call(40),
            ],
        )
        self.assertEqual(mock_create_ffmpeg.await_count, 3)
        self.assertEqual(mock_process_chunks.await_count, 3)
        self.assertEqual(mock_cleanup.await_count, 3)

    @patch(
        "backend.pipeline.ingestion.icecast_collector.asyncio.sleep",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.icecast_collector._cleanup_stream",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.icecast_collector._process_stream_chunks",
        new_callable=AsyncMock,
    )
    @patch(
        "backend.pipeline.ingestion.icecast_collector._create_ffmpeg_process",
        new_callable=AsyncMock,
    )
    async def test_monitor_stream_backoff_resets_after_single_chunk_iteration(
        self,
        mock_create_ffmpeg: AsyncMock,
        mock_process_chunks: AsyncMock,
        mock_cleanup: AsyncMock,
        mock_sleep: AsyncMock,
    ) -> None:
        """A single-chunk iteration resets backoff before the next failure."""
        mock_proc = MagicMock()
        mock_proc.pid = 654
        mock_proc.returncode = 1
        mock_create_ffmpeg.return_value = mock_proc

        # failure, failure, success(1 chunk), failure, then stop.
        mock_process_chunks.side_effect = [0, 0, 1, 0]
        mock_sleep.side_effect = [None, None, None, RuntimeError("Stop")]

        with self.assertRaises(RuntimeError):
            await icecast_collector.monitor_stream(self.mock_storage_client)

        self.assertEqual(
            mock_sleep.await_args_list,
            [
                call(10),
                call(20),
                call(5),
                call(10),
            ],
        )
        self.assertEqual(mock_create_ffmpeg.await_count, 4)
        self.assertEqual(mock_process_chunks.await_count, 4)
        self.assertEqual(mock_cleanup.await_count, 4)


if __name__ == "__main__":
    unittest.main()
