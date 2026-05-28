from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from typing import Any

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)

from dataset_split.artifacts import DatasetVersionExistsError  # noqa: E402
from dataset_split.publisher import (  # noqa: E402
    DatasetPublicationError,
    DatasetPublicationResult,
    publish_dataset_version_artifacts,
)
from dataset_split.types import LabeledSegment  # noqa: E402


class FakeListedBlob:
    def __init__(self, name: str) -> None:
        self.name = name


class FakePreconditionFailure(Exception):
    pass


class FakeBlob:
    _missing = object()

    def __init__(self, path: str, client: FakeStorageClient) -> None:
        self.path = path
        self.client = client

    def upload_from_string(
        self,
        text: str,
        *,
        content_type: str,
        if_generation_match: int | object = _missing,
    ) -> None:
        if if_generation_match is self._missing:
            raise AssertionError("if_generation_match=0 is required")
        if self.client.upload_error is not None:
            raise self.client.upload_error
        self.client.uploads.append(
            {
                "bucket_name": self.client.bucket_name,
                "path": self.path,
                "text": text,
                "content_type": content_type,
                "if_generation_match": if_generation_match,
            }
        )


class FakeBucket:
    def __init__(self, bucket_name: str, client: FakeStorageClient) -> None:
        self.bucket_name = bucket_name
        self.client = client

    def blob(self, path: str) -> FakeBlob:
        self.client.bucket_name = self.bucket_name
        return FakeBlob(path, self.client)


class FakeStorageClient:
    def __init__(
        self,
        listed_blobs: list[FakeListedBlob] | None = None,
        *,
        upload_error: Exception | None = None,
    ) -> None:
        self.listed_blobs = listed_blobs or []
        self.upload_error = upload_error
        self.list_calls: list[dict[str, Any]] = []
        self.uploads: list[dict[str, Any]] = []
        self.bucket_name = ""

    def list_blobs(
        self, bucket_name: str, *, prefix: str, max_results: int
    ) -> list[FakeListedBlob]:
        self.list_calls.append(
            {
                "bucket_name": bucket_name,
                "prefix": prefix,
                "max_results": max_results,
            }
        )
        return self.listed_blobs

    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket(bucket_name, self)


def _segment(
    source_group: str,
    *,
    split: str,
    dataset_name: str,
    dataset_family: str,
    row_index: int,
    duration: float,
    suffix: str = "flac",
) -> LabeledSegment:
    audio_uri = (
        f"gs://wd-source/{dataset_name}/{source_group}/{row_index}.{suffix}"
    )
    return LabeledSegment(
        dataset_name=dataset_name,
        dataset_family=dataset_family,
        source_strategy=dataset_family,
        source_group=source_group,
        audio_uri=audio_uri,
        original_audio_uri=audio_uri,
        text=f"{source_group} responding",
        row_index=row_index,
        offset=float(row_index),
        duration=duration,
        timestamp="2026-05-27T12:00:00Z",
        example_id=f"example-{row_index}",
        segment_id=f"segment-{row_index}",
        split=split,
    )


def _segments() -> tuple[LabeledSegment, ...]:
    return (
        _segment(
            "calls-a",
            split="train",
            dataset_name="calls",
            dataset_family="bcfy_calls",
            row_index=1,
            duration=5.0,
            suffix="flac",
        ),
        _segment(
            "calls-b",
            split="eval",
            dataset_name="calls",
            dataset_family="bcfy_calls",
            row_index=2,
            duration=7.0,
            suffix="mp3",
        ),
        _segment(
            "feeds-a",
            split="train",
            dataset_name="feeds",
            dataset_family="bcfy_feeds",
            row_index=3,
            duration=11.0,
            suffix="flac",
        ),
    )


def _publish(client: FakeStorageClient) -> DatasetPublicationResult:
    return publish_dataset_version_artifacts(
        client,
        dataset_version_id="dv-001",
        segments=_segments(),
        resolved_config={"dataset_version_id": "dv-001", "seed": 17},
        leakage_validation={"passed": True},
        balance_report={"score": 0.92, "components": {}},
        system_prompt="You are a radio transcription model.",
        user_prompt="Transcribe the emergency radio audio.",
    )


def _expected_uris() -> set[str]:
    root = "gs://wd-transcription-data/sft/dv-001"
    return {
        f"{root}/config/resolved_config.json",
        f"{root}/metadata/dataset_version.json",
        f"{root}/manifests/canonical/train.jsonl",
        f"{root}/manifests/canonical/eval.jsonl",
        f"{root}/manifests/per_dataset/calls/train.jsonl",
        f"{root}/manifests/per_dataset/calls/eval.jsonl",
        f"{root}/manifests/per_dataset/feeds/train.jsonl",
        f"{root}/manifests/per_dataset/feeds/eval.jsonl",
        f"{root}/model_inputs/nemo/train.jsonl",
        f"{root}/model_inputs/nemo/eval.jsonl",
        f"{root}/model_inputs/nemo/config.json",
        f"{root}/model_inputs/whisper/train.jsonl",
        f"{root}/model_inputs/whisper/eval.jsonl",
        f"{root}/model_inputs/gemini/train.jsonl",
        f"{root}/model_inputs/gemini/eval.jsonl",
        f"{root}/model_inputs/gemini/tuning_config.json",
        f"{root}/reports/dataset_version_report.json",
        f"{root}/reports/dataset_version_report.md",
    }


def _flatten_strings(value: object) -> set[str]:
    if isinstance(value, str):
        return {value}
    if isinstance(value, dict):
        flattened: set[str] = set()
        for nested in value.values():
            flattened.update(_flatten_strings(nested))
        return flattened
    if isinstance(value, (list, tuple)):
        flattened = set()
        for nested in value:
            flattened.update(_flatten_strings(nested))
        return flattened
    return set()


def _upload_by_uri(client: FakeStorageClient) -> dict[str, dict[str, Any]]:
    return {
        f"gs://{upload['bucket_name']}/{upload['path']}": upload
        for upload in client.uploads
    }


class TestDatasetPublisher(unittest.TestCase):
    def test_publish_dataset_version_uploads_expected_uri_inventory(
        self,
    ) -> None:
        client = FakeStorageClient()

        result = _publish(client)

        self.assertIsInstance(result, DatasetPublicationResult)
        self.assertEqual(result.dataset_version_id, "dv-001")
        self.assertEqual(
            result.root_uri, "gs://wd-transcription-data/sft/dv-001/"
        )
        self.assertEqual(
            client.list_calls,
            [
                {
                    "bucket_name": "wd-transcription-data",
                    "prefix": "sft/dv-001/",
                    "max_results": 1,
                }
            ],
        )
        self.assertEqual(
            {artifact.uri for artifact in result.artifacts},
            _expected_uris(),
        )
        self.assertEqual(set(_upload_by_uri(client)), _expected_uris())
        self.assertTrue(
            _expected_uris().issubset(
                _flatten_strings(result.artifact_inventory)
            )
        )
        self.assertEqual(
            result.artifact_inventory["audio_prefix"],
            "gs://wd-transcription-data/sft/dv-001/audio/",
        )
        self.assertEqual(
            {upload["if_generation_match"] for upload in client.uploads},
            {0},
        )
        self.assertEqual(
            _upload_by_uri(client)[
                "gs://wd-transcription-data/sft/dv-001/reports/dataset_version_report.md"
            ]["content_type"],
            "text/markdown",
        )
        self.assertEqual(
            _upload_by_uri(client)[
                "gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl"
            ]["content_type"],
            "application/x-ndjson",
        )

    def test_publish_dataset_version_report_contains_model_writer_summary(
        self,
    ) -> None:
        client = FakeStorageClient()

        _publish(client)

        report_upload = _upload_by_uri(client)[
            "gs://wd-transcription-data/sft/dv-001/reports/dataset_version_report.json"
        ]
        payload = json.loads(report_upload["text"])
        self.assertEqual(
            payload["model_writer_summary"]["nemo"]["splits"]["train"]["count"],
            2,
        )
        self.assertEqual(
            payload["model_writer_summary"]["whisper"]["splits"]["eval"][
                "duration_seconds"
            ],
            7.0,
        )
        self.assertEqual(
            payload["model_writer_summary"]["gemini"]["total"][
                "duration_seconds"
            ],
            23.0,
        )

    def test_publish_dataset_version_checks_prefix_before_upload(self) -> None:
        client = FakeStorageClient(
            [FakeListedBlob("sft/dv-001/config/resolved_config.json")]
        )

        with self.assertRaises(DatasetVersionExistsError):
            _publish(client)

        self.assertEqual(client.uploads, [])
        self.assertEqual(len(client.list_calls), 1)

    def test_publish_dataset_version_precondition_failure_fails(self) -> None:
        client = FakeStorageClient(
            upload_error=FakePreconditionFailure("object exists")
        )

        with self.assertRaises(DatasetVersionExistsError):
            _publish(client)

        self.assertEqual(len(client.list_calls), 1)
        self.assertEqual(client.uploads, [])

    def test_publish_rejects_sft_run_fields_before_upload(self) -> None:
        client = FakeStorageClient()

        with self.assertRaisesRegex(
            DatasetPublicationError,
            "resolved_config.history.0.tuned_model_id",
        ):
            publish_dataset_version_artifacts(
                client,
                dataset_version_id="dv-001",
                segments=_segments(),
                resolved_config={
                    "dataset_version_id": "dv-001",
                    "history": [{"tuned_model_id": "must-not-publish"}],
                },
                leakage_validation={"passed": True},
                balance_report={"score": 0.92, "components": {}},
                system_prompt="You are a radio transcription model.",
                user_prompt="Transcribe the emergency radio audio.",
            )

        self.assertEqual(client.list_calls, [])
        self.assertEqual(client.uploads, [])


if __name__ == "__main__":
    unittest.main()
