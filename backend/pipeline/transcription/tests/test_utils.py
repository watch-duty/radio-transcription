import unittest
from unittest.mock import MagicMock, patch

from backend.pipeline.schema_types.sed_metadata_pb2 import (
    SedMetadata,
)
from backend.pipeline.transcription.utils import (
    get_gcs_client,
    read_sed_segments_from_blob,
)
import base64


class TestUtils(unittest.TestCase):
    @patch("backend.pipeline.transcription.utils.storage.Client")
    def test_get_gcs_client(self, mock_client: MagicMock) -> None:
        client = get_gcs_client()
        self.assertIsNotNone(client)
        mock_client.assert_called_once()

    def test_read_sed_segments_from_blob_success(self) -> None:
        mock_blob = MagicMock()

        sed_metadata = SedMetadata()
        seg1 = sed_metadata.sound_events.add()
        seg1.start_time.seconds = 0
        seg1.start_time.nanos = 500000000
        seg1.duration.seconds = 1
        seg1.duration.nanos = 0

        metadata_bytes = sed_metadata.SerializeToString()
        mock_blob.metadata = {"sed_metadata": base64.b64encode(metadata_bytes).decode('utf-8')}

        segments = read_sed_segments_from_blob(mock_blob)
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0], (0.5, 1.5))
        mock_blob.reload.assert_called_once()

    def test_read_sed_segments_from_blob_not_found(self) -> None:
        mock_blob = MagicMock()
        mock_blob.metadata = {"other_data": "value"}
        mock_blob.name = "test.wav"

        with self.assertRaises(FileNotFoundError):
            read_sed_segments_from_blob(mock_blob)

        mock_blob.reload.assert_called_once()


if __name__ == "__main__":
    unittest.main()
