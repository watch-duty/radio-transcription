"""Tests for the review-pool builder CLI."""

from __future__ import annotations

import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest
import unittest.mock

_REVIEW_DIR = str(pathlib.Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    pathlib.Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _REVIEW_DIR not in sys.path:
    sys.path.insert(0, _REVIEW_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)


def _canonical_row(
    audio_uri: str,
    *,
    split: str = "train",
    row_index: int = 1,
    text: str = "Engine 41 copy",
) -> dict[str, object]:
    return {
        "model_ready_audio_uri": audio_uri,
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": 1.25,
        "duration": 2.5,
        "source_group": "source:1",
        "row_index": row_index,
        "split": split,
        "dataset_name": "test-dataset",
        "text": text,
    }


def _metadata(
    uri: str,
    *,
    md5_hash: str,
    size: int = 321,
) -> object:
    import build_review_pool

    return build_review_pool.review.GcsObjectMetadata(
        uri=uri,
        md5_hash=md5_hash,
        crc32c_hash=f"crc-{md5_hash}",
        size=size,
        generation="7",
        storage_url=uri,
    )


def _write_jsonl(
    path: pathlib.Path,
    rows: list[dict[str, object]],
) -> None:
    with path.open("w", encoding="utf-8") as output_file:
        for row in rows:
            output_file.write(json.dumps(row, sort_keys=True))
            output_file.write("\n")


def _read_jsonl(path: pathlib.Path) -> list[dict[str, object]]:
    rows = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if raw_line.strip():
            rows.append(json.loads(raw_line))
    return rows


class TestBuildReviewPoolCli(unittest.TestCase):
    def test_help_lists_expected_arguments(self) -> None:
        import build_review_pool

        output = io.StringIO()
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(output):
                build_review_pool.main(["--help"])

        self.assertEqual(ctx.exception.code, 0)
        help_text = output.getvalue()
        self.assertIn("--train-jsonl", help_text)
        self.assertIn("--eval-jsonl", help_text)
        self.assertIn("--review-pool-jsonl", help_text)
        self.assertIn("--duplicates-jsonl", help_text)
        self.assertIn("--metadata-workers", help_text)

    def test_local_success_writes_review_pool_and_empty_duplicates(
        self,
    ) -> None:
        import build_review_pool

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            train_path = tmp_path / "train.jsonl"
            eval_path = tmp_path / "eval.jsonl"
            review_pool_path = tmp_path / "review_pool.jsonl"
            duplicates_path = tmp_path / "duplicates.jsonl"
            train_row = _canonical_row(
                "gs://bucket/train.flac",
                split="train",
                row_index=1,
                text="Train transcript",
            )
            eval_row = _canonical_row(
                "gs://bucket/eval.flac",
                split="eval",
                row_index=2,
                text="Eval transcript",
            )
            _write_jsonl(train_path, [train_row])
            _write_jsonl(eval_path, [eval_row])

            def fake_fetch_metadata(
                _storage_client: object, uri: str
            ) -> object:
                return _metadata(uri, md5_hash=uri.rsplit("/", 1)[-1])

            with (
                unittest.mock.patch.object(
                    build_review_pool,
                    "_new_storage_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    build_review_pool.review,
                    "fetch_gcs_object_metadata",
                    fake_fetch_metadata,
                ),
            ):
                exit_code = build_review_pool.main(
                    [
                        "--train-jsonl",
                        str(train_path),
                        "--eval-jsonl",
                        str(eval_path),
                        "--review-pool-jsonl",
                        str(review_pool_path),
                        "--duplicates-jsonl",
                        str(duplicates_path),
                    ]
                )

            review_pool_rows = _read_jsonl(review_pool_path)
            duplicate_rows = _read_jsonl(duplicates_path)

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(review_pool_rows), 2)
        self.assertEqual(
            [row["model_ready_audio_uri"] for row in review_pool_rows],
            ["gs://bucket/train.flac", "gs://bucket/eval.flac"],
        )
        self.assertEqual(
            [row["split"] for row in review_pool_rows],
            [
                "train",
                "eval",
            ],
        )
        self.assertIn("audio_segment_id", review_pool_rows[0])
        self.assertIn("source_window_id", review_pool_rows[0])
        self.assertEqual(review_pool_rows[0]["md5_hash"], "train.flac")
        self.assertEqual(duplicate_rows, [])

    def test_duplicate_audio_ids_write_report_and_nonzero_without_pool(
        self,
    ) -> None:
        import build_review_pool

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            train_path = tmp_path / "train.jsonl"
            eval_path = tmp_path / "eval.jsonl"
            review_pool_path = tmp_path / "review_pool.jsonl"
            duplicates_path = tmp_path / "duplicates.jsonl"
            _write_jsonl(
                train_path,
                [
                    _canonical_row("gs://bucket/a.flac", row_index=1),
                    _canonical_row("gs://bucket/b.flac", row_index=2),
                ],
            )
            _write_jsonl(eval_path, [])

            def fake_fetch_metadata(
                _storage_client: object, uri: str
            ) -> object:
                return _metadata(uri, md5_hash="same-md5")

            with (
                unittest.mock.patch.object(
                    build_review_pool,
                    "_new_storage_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    build_review_pool.review,
                    "fetch_gcs_object_metadata",
                    fake_fetch_metadata,
                ),
            ):
                exit_code = build_review_pool.main(
                    [
                        "--train-jsonl",
                        str(train_path),
                        "--eval-jsonl",
                        str(eval_path),
                        "--review-pool-jsonl",
                        str(review_pool_path),
                        "--duplicates-jsonl",
                        str(duplicates_path),
                    ]
                )

            duplicate_rows = _read_jsonl(duplicates_path)
            review_pool_text = (
                review_pool_path.read_text(encoding="utf-8")
                if review_pool_path.exists()
                else ""
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(len(duplicate_rows), 2)
        self.assertEqual(
            [row["model_ready_audio_uri"] for row in duplicate_rows],
            ["gs://bucket/a.flac", "gs://bucket/b.flac"],
        )
        self.assertEqual(review_pool_text, "")

    def test_metadata_workers_argument_controls_fetch_concurrency(
        self,
    ) -> None:
        import build_review_pool

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(tmpdir)
            train_path = tmp_path / "train.jsonl"
            eval_path = tmp_path / "eval.jsonl"
            review_pool_path = tmp_path / "review_pool.jsonl"
            duplicates_path = tmp_path / "duplicates.jsonl"
            _write_jsonl(train_path, [_canonical_row("gs://bucket/a.flac")])
            _write_jsonl(eval_path, [])
            captured: dict[str, object] = {}

            def fake_fetch_metadata_by_uri(
                _storage_client: object,
                manifest_rows: list[object],
                *,
                max_workers: int,
            ) -> dict[str, object]:
                captured["max_workers"] = max_workers
                row = manifest_rows[0]
                uri = row.model_ready_audio_uri
                return {uri: _metadata(uri, md5_hash="a.flac")}

            with (
                unittest.mock.patch.object(
                    build_review_pool,
                    "_new_storage_client",
                    return_value=object(),
                ),
                unittest.mock.patch.object(
                    build_review_pool,
                    "_fetch_metadata_by_uri",
                    fake_fetch_metadata_by_uri,
                ),
            ):
                exit_code = build_review_pool.main(
                    [
                        "--train-jsonl",
                        str(train_path),
                        "--eval-jsonl",
                        str(eval_path),
                        "--review-pool-jsonl",
                        str(review_pool_path),
                        "--duplicates-jsonl",
                        str(duplicates_path),
                        "--metadata-workers",
                        "7",
                    ]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured["max_workers"], 7)

    def test_gcs_input_and_output_wiring(self) -> None:
        import build_review_pool

        uploaded_paths: list[str] = []
        uploaded_text_by_path: dict[str, str] = {}

        def fake_download_jsonl_manifest(
            _storage_client: object,
            manifest_uri: str,
        ) -> list[dict[str, object]]:
            if manifest_uri.endswith("/train.jsonl"):
                return [_canonical_row("gs://bucket/train.flac")]
            return [
                _canonical_row(
                    "gs://bucket/eval.flac",
                    split="eval",
                    row_index=2,
                )
            ]

        def fake_fetch_metadata(_storage_client: object, uri: str) -> object:
            return _metadata(uri, md5_hash=uri.rsplit("/", 1)[-1])

        def fake_upload_file_to_blob(
            _storage_client: object,
            bucket_name: str,
            blob_path: str,
            source_file_name: str,
        ) -> None:
            uploaded_path = f"gs://{bucket_name}/{blob_path}"
            uploaded_paths.append(uploaded_path)
            uploaded_text_by_path[uploaded_path] = pathlib.Path(
                source_file_name
            ).read_text(encoding="utf-8")

        with (
            unittest.mock.patch.object(
                build_review_pool,
                "_new_storage_client",
                return_value=object(),
            ),
            unittest.mock.patch.object(
                build_review_pool.gcs_utils,
                "download_jsonl_manifest",
                fake_download_jsonl_manifest,
            ),
            unittest.mock.patch.object(
                build_review_pool.review,
                "fetch_gcs_object_metadata",
                fake_fetch_metadata,
            ),
            unittest.mock.patch.object(
                build_review_pool.gcs_utils,
                "upload_file_to_blob",
                fake_upload_file_to_blob,
            ),
        ):
            exit_code = build_review_pool.main(
                [
                    "--train-jsonl",
                    "gs://bucket/input/train.jsonl",
                    "--eval-jsonl",
                    "gs://bucket/input/eval.jsonl",
                    "--review-pool-jsonl",
                    "gs://bucket/output/review_pool.jsonl",
                    "--duplicates-jsonl",
                    "gs://bucket/output/duplicates.jsonl",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            uploaded_paths,
            [
                "gs://bucket/output/review_pool.jsonl",
                "gs://bucket/output/duplicates.jsonl",
            ],
        )
        self.assertIn(
            '"audio_segment_id": "audio-',
            uploaded_text_by_path["gs://bucket/output/review_pool.jsonl"],
        )
        self.assertEqual(
            uploaded_text_by_path["gs://bucket/output/duplicates.jsonl"],
            "",
        )


if __name__ == "__main__":
    unittest.main()
