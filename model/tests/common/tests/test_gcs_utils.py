import tempfile
import unittest
import unittest.mock
from pathlib import Path

from common import gcs_utils as gcs_utils_lib
from common.gcs_utils import (
    download_gcs_directory,
    download_gcs_uri,
    download_json_text,
    download_to_scratch,
    gcs_prefix_has_any_blob,
    gcs_uri_exists,
    upload_local_file,
    upload_text,
)
from fake_gcs import FakeStorageClient


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


class TestGcsObjectHelpers(unittest.TestCase):
    def test_gcs_uri_exists_returns_object_existence(self) -> None:
        storage = FakeStorageClient()
        storage.put("gs://bucket/path/file.json", "{}")

        self.assertTrue(gcs_uri_exists(storage, "gs://bucket/path/file.json"))
        self.assertFalse(
            gcs_uri_exists(storage, "gs://bucket/path/missing.json")
        )

    def test_gcs_prefix_has_any_blob_checks_prefix(self) -> None:
        storage = FakeStorageClient()
        storage.put("gs://bucket/runs/a/config.json", "{}")

        self.assertTrue(gcs_prefix_has_any_blob(storage, "gs://bucket/runs/a/"))
        self.assertFalse(
            gcs_prefix_has_any_blob(storage, "gs://bucket/runs/b/")
        )

    def test_download_json_text_requires_json_object(self) -> None:
        storage = FakeStorageClient()
        storage.put("gs://bucket/config.json", '{"status": "ok"}')
        storage.put("gs://bucket/list.json", '["not", "object"]')

        self.assertEqual(
            download_json_text(storage, "gs://bucket/config.json"),
            {"status": "ok"},
        )
        with self.assertRaisesRegex(TypeError, "Expected JSON object"):
            download_json_text(storage, "gs://bucket/list.json")

    def test_download_jsonl_manifest_uses_shared_lenient_parser(self) -> None:
        storage = FakeStorageClient()
        storage.put(
            "gs://bucket/manifest/eval.jsonl",
            '{"audio_filepath": "gs://b/a.flac", "text": null}\n'
            "{bad json}\n"
            '["not", "an", "object"]\n'
            '{"audio_filepath": "gs://b/b.flac", "text": "line\\nbreak"}',
        )

        rows = gcs_utils_lib.download_jsonl_manifest(
            storage,
            "gs://bucket/manifest/eval.jsonl",
        )

        self.assertEqual(
            rows,
            [
                {"audio_filepath": "gs://b/a.flac", "text": ""},
                {"audio_filepath": "gs://b/b.flac", "text": "line break"},
            ],
        )

    def test_strict_manifest_download_rejects_malformed_line(self) -> None:
        storage = FakeStorageClient()
        uri = "gs://bucket/manifest/eval.jsonl"
        storage.put(
            uri,
            '{"audio_filepath":"gs://b/a.flac","text":"ok"}\n{bad json}\n',
        )

        with self.assertRaisesRegex(
            ValueError,
            r"gs://bucket/manifest/eval.jsonl: malformed JSON at line 2",
        ):
            gcs_utils_lib.download_jsonl_manifest_strict(storage, uri)

    def test_upload_and_download_local_text_artifacts(self) -> None:
        storage = FakeStorageClient()
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp = Path(tmp_s)
            source = tmp / "source.txt"
            dest = tmp / "dest.txt"
            source.write_text("hello", encoding="utf-8")

            upload_local_file(storage, source, "gs://bucket/path/source.txt")
            download_gcs_uri(storage, "gs://bucket/path/source.txt", dest)
            upload_text(storage, "inline", "gs://bucket/path/inline.txt")

            self.assertEqual(dest.read_text(encoding="utf-8"), "hello")
            self.assertEqual(
                storage.get("gs://bucket/path/inline.txt"), "inline"
            )

    def test_download_gcs_directory(self) -> None:
        storage = FakeStorageClient()
        storage.put("gs://bucket/dir/a.txt", "file a")
        storage.put("gs://bucket/dir/subdir/b.txt", "file b")
        storage.put("gs://bucket/other.txt", "other file")

        with tempfile.TemporaryDirectory() as tmp_s:
            tmp_dir = Path(tmp_s)
            download_gcs_directory(storage, "gs://bucket/dir", tmp_dir)

            self.assertEqual(
                (tmp_dir / "a.txt").read_text(encoding="utf-8"), "file a"
            )
            self.assertEqual(
                (tmp_dir / "subdir/b.txt").read_text(encoding="utf-8"), "file b"
            )
            self.assertFalse((tmp_dir / "other.txt").exists())

    def test_download_gcs_directory_not_found(self) -> None:
        storage = FakeStorageClient()
        with tempfile.TemporaryDirectory() as tmp_s:
            tmp_dir = Path(tmp_s)
            with self.assertRaises(FileNotFoundError):
                download_gcs_directory(
                    storage, "gs://bucket/nonexistent", tmp_dir
                )


if __name__ == "__main__":
    unittest.main()
