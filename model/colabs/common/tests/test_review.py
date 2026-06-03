"""Tests for Phase 1 review-row identity helpers."""

import unittest


def _row(
    audio_uri: str,
    text: str,
    *,
    original_audio_uri: str = "gs://bucket/original.wav",
    offset: object = 1.25,
    duration: object = 2.5,
    source_group: str = "source:1",
    row_index: int = 7,
    split: str = "train",
) -> dict[str, object]:
    return {
        "model_ready_audio_uri": audio_uri,
        "original_audio_uri": original_audio_uri,
        "offset": offset,
        "duration": duration,
        "source_group": source_group,
        "row_index": row_index,
        "split": split,
        "dataset_name": "test",
        "text": text,
    }


class _FakeBlob:
    def __init__(
        self,
        *,
        md5_hash: str | None = "md5",
        crc32c: str | None = "crc",
        size: int | None = 123,
        generation: str | None = "9",
    ) -> None:
        self.reload_calls = 0
        self._md5_hash = md5_hash
        self._crc32c = crc32c
        self._size = size
        self._generation = generation

    def reload(self, **_kwargs: object) -> None:
        self.reload_calls += 1

    def _ensure_reloaded(self) -> None:
        if not self.reload_calls:
            raise RuntimeError("metadata read before reload")

    @property
    def md5_hash(self) -> str | None:
        self._ensure_reloaded()
        return self._md5_hash

    @property
    def crc32c(self) -> str | None:
        self._ensure_reloaded()
        return self._crc32c

    @property
    def size(self) -> int | None:
        self._ensure_reloaded()
        return self._size

    @property
    def generation(self) -> str | None:
        self._ensure_reloaded()
        return self._generation


class _FakeBucket:
    def __init__(self, blob: _FakeBlob) -> None:
        self.blob_path = ""
        self._blob = blob

    def blob(self, blob_path: str) -> _FakeBlob:
        self.blob_path = blob_path
        return self._blob


class _FakeStorageClient:
    def __init__(self, blob: _FakeBlob) -> None:
        self.bucket_name = ""
        self.bucket_obj = _FakeBucket(blob)

    def bucket(self, bucket_name: str) -> _FakeBucket:
        self.bucket_name = bucket_name
        return self.bucket_obj


class TestReviewManifestLoading(unittest.TestCase):
    def test_load_review_manifest_rows_combines_manifests_in_order(
        self,
    ) -> None:
        from common import review

        rows = review.load_review_manifest_rows(
            {
                "manifest-1": [_row("gs://bucket/first.flac", "First")],
                "manifest-2": [_row("gs://bucket/second.flac", "Second")],
            }
        )

        self.assertEqual(
            [row.model_ready_audio_uri for row in rows],
            ["gs://bucket/first.flac", "gs://bucket/second.flac"],
        )

    def test_load_review_manifest_rows_missing_required_field_raises(
        self,
    ) -> None:
        from common import review

        row = _row("gs://bucket/missing.flac", "Missing")
        del row["model_ready_audio_uri"]

        with self.assertRaisesRegex(ValueError, "model_ready_audio_uri"):
            review.load_review_manifest_rows({"train": [row]})

    def test_load_review_manifest_rows_null_required_field_raises(
        self,
    ) -> None:
        from common import review

        row = _row("gs://bucket/null.flac", "Null")
        row["text"] = None

        with self.assertRaisesRegex(ValueError, "text"):
            review.load_review_manifest_rows({"manifest-1": [row]})


class TestGcsObjectMetadata(unittest.TestCase):
    def test_fetch_gcs_object_metadata_reloads_before_reading(self) -> None:
        from common import review

        blob = _FakeBlob(md5_hash="md5-a", crc32c="crc-a", size=456)
        client = _FakeStorageClient(blob)

        metadata = review.fetch_gcs_object_metadata(
            client,
            "gs://bucket/path/audio.flac",
        )

        self.assertEqual(blob.reload_calls, 1)
        self.assertEqual(client.bucket_name, "bucket")
        self.assertEqual(client.bucket_obj.blob_path, "path/audio.flac")
        self.assertEqual(metadata.md5_hash, "md5-a")
        self.assertEqual(metadata.crc32c_hash, "crc-a")
        self.assertEqual(metadata.size, 456)
        self.assertEqual(metadata.storage_url, "gs://bucket/path/audio.flac")

    def test_fetch_gcs_object_metadata_missing_md5_raises(self) -> None:
        from common import review

        blob = _FakeBlob(md5_hash=None)

        with self.assertRaisesRegex(ValueError, "md5_hash"):
            review.fetch_gcs_object_metadata(
                _FakeStorageClient(blob),
                "gs://bucket/no-md5.flac",
            )


class TestReviewIdentity(unittest.TestCase):
    def test_audio_segment_id_uses_content_metadata_not_uri(self) -> None:
        from common import review

        first = review.GcsObjectMetadata(
            uri="gs://bucket/a.flac",
            md5_hash="same-md5",
            crc32c_hash="crc-a",
            size=123,
            generation="1",
            storage_url="gs://bucket/a.flac",
        )
        moved = review.GcsObjectMetadata(
            uri="gs://other/path/a.flac",
            md5_hash="same-md5",
            crc32c_hash="crc-b",
            size=123,
            generation="2",
            storage_url="gs://other/path/a.flac",
        )
        changed_md5 = review.GcsObjectMetadata(
            uri="gs://bucket/a.flac",
            md5_hash="other-md5",
            crc32c_hash="crc-a",
            size=123,
            generation="1",
            storage_url="gs://bucket/a.flac",
        )
        changed_size = review.GcsObjectMetadata(
            uri="gs://bucket/a.flac",
            md5_hash="same-md5",
            crc32c_hash="crc-a",
            size=124,
            generation="1",
            storage_url="gs://bucket/a.flac",
        )

        self.assertEqual(
            review.audio_segment_id(first), review.audio_segment_id(moved)
        )
        self.assertNotEqual(
            review.audio_segment_id(first),
            review.audio_segment_id(changed_md5),
        )
        self.assertNotEqual(
            review.audio_segment_id(first),
            review.audio_segment_id(changed_size),
        )

    def test_source_window_id_uses_source_timing_and_group(self) -> None:
        from common import review

        first = _row("gs://bucket/a.flac", "Engine 41")
        moved = _row("gs://bucket/other.flac", "Different text")
        changed_window = _row(
            "gs://bucket/a.flac",
            "Engine 41",
            offset=1.26,
        )

        self.assertEqual(
            review.source_window_id(first), review.source_window_id(moved)
        )
        self.assertNotEqual(
            review.source_window_id(first),
            review.source_window_id(changed_window),
        )

    def test_source_window_id_normalizes_equivalent_numeric_values(
        self,
    ) -> None:
        from common import review

        int_row = _row("gs://bucket/a.flac", "A", offset=1, duration=2)
        float_row = _row("gs://bucket/a.flac", "B", offset=1.0, duration=2.0)
        string_row = _row(
            "gs://bucket/a.flac",
            "C",
            offset="1.000",
            duration="2.000",
        )

        self.assertEqual(
            review.source_window_id(int_row),
            review.source_window_id(float_row),
        )
        self.assertEqual(
            review.source_window_id(float_row),
            review.source_window_id(string_row),
        )


class TestReviewPoolAssembly(unittest.TestCase):
    def test_build_review_pool_attaches_ids_and_audit_metadata(self) -> None:
        from common import review

        rows = review.load_review_manifest_rows(
            {"train": [_row("gs://bucket/a.flac", "Engine 41")]}
        )
        metadata = review.GcsObjectMetadata(
            uri="gs://bucket/a.flac",
            md5_hash="md5-a",
            crc32c_hash="crc-a",
            size=321,
            generation="7",
            storage_url="gs://bucket/a.flac",
        )

        pool = review.build_review_pool(
            rows,
            metadata_by_uri={"gs://bucket/a.flac": metadata},
        )

        self.assertEqual(len(pool), 1)
        item = pool[0]
        self.assertEqual(
            item["audio_segment_id"],
            review.audio_segment_id(metadata),
        )
        self.assertEqual(
            item["source_window_id"],
            review.source_window_id(rows[0]),
        )
        self.assertEqual(item["model_ready_audio_uri"], "gs://bucket/a.flac")
        self.assertEqual(item["original_audio_uri"], "gs://bucket/original.wav")
        self.assertEqual(item["offset"], 1.25)
        self.assertEqual(item["duration"], 2.5)
        self.assertEqual(item["source_group"], "source:1")
        self.assertEqual(item["row_index"], 7)
        self.assertNotIn("split", item)
        self.assertEqual(item["dataset_name"], "test")
        self.assertEqual(item["text"], "Engine 41")
        self.assertEqual(item["md5_hash"], "md5-a")
        self.assertEqual(item["crc32c_hash"], "crc-a")
        self.assertEqual(item["size"], 321)
        self.assertEqual(item["generation"], "7")
        self.assertEqual(item["storage_url"], "gs://bucket/a.flac")

    def test_build_review_pool_missing_metadata_raises(self) -> None:
        from common import review

        rows = review.load_review_manifest_rows(
            {"train": [_row("gs://bucket/missing.flac", "Missing")]}
        )

        with self.assertRaisesRegex(ValueError, "gs://bucket/missing.flac"):
            review.build_review_pool(rows, metadata_by_uri={})

    def test_build_review_pool_preserves_manifest_audio_uri(self) -> None:
        from common import review

        rows = review.load_review_manifest_rows(
            {"train": [_row("gs://bucket/manifest.flac", "Manifest")]}
        )
        metadata = review.GcsObjectMetadata(
            uri="gs://bucket/stale-metadata.flac",
            md5_hash="md5-manifest",
            crc32c_hash="crc-manifest",
            size=333,
            generation="8",
            storage_url="gs://bucket/stale-metadata.flac",
        )

        pool = review.build_review_pool(
            rows,
            metadata_by_uri={"gs://bucket/manifest.flac": metadata},
        )

        self.assertEqual(
            pool[0]["model_ready_audio_uri"],
            "gs://bucket/manifest.flac",
        )


class TestDuplicateAudioSegments(unittest.TestCase):
    def test_duplicate_audio_segment_ids_raise_with_report_rows(self) -> None:
        from common import review

        rows = review.load_review_manifest_rows(
            {
                "train": [
                    _row("gs://bucket/a.flac", "Same", row_index=1),
                    _row("gs://bucket/b.flac", "Same", row_index=2),
                ]
            }
        )
        metadata_by_uri = {
            "gs://bucket/a.flac": review.GcsObjectMetadata(
                "gs://bucket/a.flac",
                "same-md5",
                "crc-a",
                100,
                "1",
                "gs://bucket/a.flac",
            ),
            "gs://bucket/b.flac": review.GcsObjectMetadata(
                "gs://bucket/b.flac",
                "same-md5",
                "crc-b",
                100,
                "2",
                "gs://bucket/b.flac",
            ),
        }

        with self.assertRaises(review.DuplicateAudioSegmentError) as ctx:
            review.build_review_pool(rows, metadata_by_uri=metadata_by_uri)

        duplicate_rows = ctx.exception.duplicates
        self.assertEqual(len(duplicate_rows), 2)
        self.assertEqual(
            set(duplicate_rows[0]),
            {
                "audio_segment_id",
                "model_ready_audio_uri",
                "original_audio_uri",
                "offset",
                "duration",
                "source_group",
                "row_index",
                "dataset_name",
                "text",
            },
        )
        self.assertEqual(
            [row["model_ready_audio_uri"] for row in duplicate_rows],
            ["gs://bucket/a.flac", "gs://bucket/b.flac"],
        )
        self.assertEqual(
            [row["text"] for row in duplicate_rows],
            ["Same", "Same"],
        )


if __name__ == "__main__":
    unittest.main()
