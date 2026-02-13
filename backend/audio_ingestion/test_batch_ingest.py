import json
import os
import tempfile
import unittest
from pathlib import Path
from types import TracebackType
from unittest.mock import MagicMock, patch

import apache_beam as beam
from apache_beam.io import fileio, filesystem
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from backend.audio_ingestion.batch_ingest import ProcessAudioDataDoFn


class DummyBlob:
    def __init__(self, metadata: dict | None) -> None:
        self.metadata = metadata

    def reload(self) -> None:
        pass


class DummyBatch:
    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        return False


class DummyBucket:
    def __init__(self, blob: DummyBlob) -> None:
        self._blob = blob

    def blob(self, blob_name: str) -> DummyBlob:
        return self._blob

    def get_blob(self, blob_name: str) -> DummyBlob:
        return self._blob


class DummyStorageClient:
    def __init__(self, blob: DummyBlob) -> None:
        self._blob = blob

    def bucket(self, bucket_name: str) -> DummyBucket:
        return DummyBucket(self._blob)

    def batch(self) -> DummyBatch:
        return DummyBatch()


class TestProcessAudioData(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_file_path = os.path.join(self.temp_dir.name, "audio.wav")
        with open(self.test_file_path, "wb") as f:
            f.write(b"fake audio")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_process_with_metadata_fields(self) -> None:
        metadata = filesystem.FileMetadata(
            path=self.test_file_path,
            size_in_bytes=Path(self.test_file_path).stat().st_size,
        )

        dofn = ProcessAudioDataDoFn()
        dummy_blob = DummyBlob(
            metadata={"location": "CA", "feed": "feed1", "source": "src1"}
        )
        dofn.storage_client = DummyStorageClient(dummy_blob)
        element = fileio.ReadableFile(metadata=metadata)
        result = list(dofn.process([element]))

        self.assertEqual(len(result), 1)
        data = json.loads(result[0].decode("utf-8"))
        self.assertEqual(data["byte_length"], 10)
        self.assertEqual(data["location"], "CA")
        self.assertEqual(data["feed"], "feed1")
        self.assertEqual(data["source"], "src1")

    def test_process_without_metadata_fields(self) -> None:
        metadata = filesystem.FileMetadata(
            path=self.test_file_path,
            size_in_bytes=Path(self.test_file_path).stat().st_size,
        )

        dofn = ProcessAudioDataDoFn()
        dummy_blob = DummyBlob(metadata=None)
        dofn.storage_client = DummyStorageClient(dummy_blob)
        element = fileio.ReadableFile(metadata=metadata)
        result = list(dofn.process([element]))

        self.assertEqual(len(result), 1)
        data = json.loads(result[0].decode("utf-8"))
        self.assertEqual(data["byte_length"], 10)
        self.assertIsNone(data.get("location"))
        self.assertIsNone(data.get("feed"))
        self.assertIsNone(data.get("source"))

    def test_process_missing_metadata_path(self) -> None:
        dofn = ProcessAudioDataDoFn()
        element = fileio.ReadableFile(metadata=None)
        result = list(dofn.process([element]))

        self.assertEqual(result, [])

    def test_process_unexpected_path_format(self) -> None:
        metadata = filesystem.FileMetadata(
            path="unexpected_format_path",
            size_in_bytes=Path(self.test_file_path).stat().st_size,
        )

        dofn = ProcessAudioDataDoFn()
        dofn.storage_client = DummyStorageClient(DummyBlob(metadata=None))
        result = list(dofn.process([fileio.ReadableFile(metadata=metadata)]))
        self.assertEqual(result, [])

    @patch("google.cloud.storage.Client")
    def test_pipeline_integration(self, mock_storage_client: MagicMock) -> None:
        # 1. Setup Mock for GCS Blob Metadata
        mock_client_instance = mock_storage_client.return_value
        mock_bucket = mock_client_instance.bucket.return_value
        mock_blob = mock_bucket.blob.return_value
        mock_blob.metadata = {"location": "CA", "feed": "feed1", "source": "src1"}
        mock_client_instance.batch.return_value.__enter__.return_value = None
        mock_client_instance.batch.return_value.__exit__.return_value = None

        with TestPipeline() as p:
            results = (
                p
                | fileio.MatchFiles(self.test_file_path)
                | fileio.ReadMatches()
                | beam.BatchElements(min_batch_size=1, max_batch_size=5)
                | beam.ParDo(ProcessAudioDataDoFn())
            )

            def check_output(elements: list[bytes]) -> None:
                self.assertEqual(len(elements), 1)
                data = json.loads(elements[0].decode("utf-8"))
                self.assertIn("byte_length", data)
                self.assertEqual(data["byte_length"], 10)
                self.assertEqual(data["location"], "CA")
                self.assertEqual(data["feed"], "feed1")
                self.assertEqual(data["source"], "src1")

            assert_that(results, check_output)


if __name__ == "__main__":
    unittest.main()
