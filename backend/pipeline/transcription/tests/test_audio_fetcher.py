import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.transcription.audio_fetcher import GCSAudioFetcher


class TestGCSAudioFetcher(unittest.TestCase):
    @patch("google.cloud.storage.Blob")
    @patch("google.cloud.storage.Client")
    def test_fetch_downloads_audio_from_gcs(
        self, mock_storage_client, mock_blob
    ) -> None:
        # Arrange
        mock_audio_bytes = b"test audio data"
        gcs_uri = "gs://test-bucket/test-audio.wav"

        mock_blob_instance = MagicMock()
        mock_blob_instance.download_as_bytes.return_value = mock_audio_bytes
        mock_blob.from_string.return_value = mock_blob_instance

        fetcher = GCSAudioFetcher(storage_client=mock_storage_client)

        # Act
        audio_bytes = fetcher.fetch(gcs_uri)

        # Assert
        self.assertEqual(audio_bytes, mock_audio_bytes)
        mock_blob.from_string.assert_called_once_with(
            gcs_uri, client=mock_storage_client
        )
        mock_blob_instance.download_as_bytes.assert_called_once()


if __name__ == "__main__":
    unittest.main()
