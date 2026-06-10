import tempfile
import unittest
import unittest.mock
from pathlib import Path

from common.gcs_utils import download_to_scratch


class TestDownloadToScratch(unittest.TestCase):
    """Tests for common.gcs_utils.download_to_scratch — no network access."""

    def _make_fake_download(self):
        """Return a patch target and a side_effect that writes a 1-byte marker."""

        def fake_download(storage_client, bucket_name, blob_path, local_path):
            with open(local_path, "wb") as f:
                f.write(b"\x00")

        return fake_download

    def test_returns_path_within_scratch_dir(self) -> None:
        """Result path's parent directory equals the scratch_dir passed in."""
        fake_download = self._make_fake_download()
        mock_client = unittest.mock.MagicMock()

        with tempfile.TemporaryDirectory() as scratch_dir:
            with unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download,
            ):
                result = download_to_scratch(
                    mock_client, "gs://bucket/path/audio.flac", scratch_dir
                )
            self.assertEqual(Path(result).parent, Path(scratch_dir))

    def test_file_is_created(self) -> None:
        """The returned path exists on disk after the call."""
        fake_download = self._make_fake_download()
        mock_client = unittest.mock.MagicMock()

        with tempfile.TemporaryDirectory() as scratch_dir:
            with unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download,
            ):
                result = download_to_scratch(
                    mock_client, "gs://bucket/path/audio.flac", scratch_dir
                )
            self.assertTrue(Path(result).exists())

    def test_two_calls_yield_distinct_paths(self) -> None:
        """Two calls with the same URI return different local paths (mkstemp uniqueness)."""
        fake_download = self._make_fake_download()
        mock_client = unittest.mock.MagicMock()

        with tempfile.TemporaryDirectory() as scratch_dir:
            with unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download,
            ):
                result1 = download_to_scratch(
                    mock_client, "gs://bucket/path/audio.flac", scratch_dir
                )
                result2 = download_to_scratch(
                    mock_client, "gs://bucket/path/audio.flac", scratch_dir
                )
            self.assertNotEqual(result1, result2)

    def test_same_basename_different_folders_yields_distinct_paths(
        self,
    ) -> None:
        """GCS objects sharing a basename in different folders get different local paths."""
        fake_download = self._make_fake_download()
        mock_client = unittest.mock.MagicMock()

        with tempfile.TemporaryDirectory() as scratch_dir:
            with unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download,
            ):
                result1 = download_to_scratch(
                    mock_client, "gs://b/a/seg.flac", scratch_dir
                )
                result2 = download_to_scratch(
                    mock_client, "gs://b/c/seg.flac", scratch_dir
                )
            self.assertNotEqual(result1, result2)

    def test_extension_preserved(self) -> None:
        """The returned local path ends with the source object's extension."""
        fake_download = self._make_fake_download()
        mock_client = unittest.mock.MagicMock()

        with tempfile.TemporaryDirectory() as scratch_dir:
            with unittest.mock.patch(
                "common.gcs_utils.download_blob_to_file",
                side_effect=fake_download,
            ):
                result = download_to_scratch(
                    mock_client, "gs://b/x.flac", scratch_dir
                )
            self.assertTrue(result.endswith(".flac"))

    def test_non_gs_uri_raises_value_error(self) -> None:
        """A bare (non-gs://) URI raises ValueError (propagated from parse_gcs_uri)."""
        mock_client = unittest.mock.MagicMock()

        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaises(ValueError):
                download_to_scratch(
                    mock_client, "/local/path/audio.flac", scratch_dir
                )


if __name__ == "__main__":
    unittest.main()
