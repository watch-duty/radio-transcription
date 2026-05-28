from __future__ import annotations

import json
import math
import sys
import unittest
from dataclasses import replace
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
from dataset_split.audio import (  # noqa: E402
    AudioPreparationResult,
    AudioUploadTask,
)
from dataset_split.publisher import (  # noqa: E402
    DatasetPublicationError,
    DatasetPublicationResult,
    publish_dataset_version_artifacts,
)
from dataset_split.types import ExcludedRow, LabeledSegment  # noqa: E402


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
        self.client.events.append(f"upload_text:{self.path}")
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
        self.events: list[str] = []
        self.bucket_name = ""

    def list_blobs(
        self, bucket_name: str, *, prefix: str, max_results: int
    ) -> list[FakeListedBlob]:
        self.events.append(f"list:{prefix}")
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
    audio_uri: str | None = None,
) -> LabeledSegment:
    if audio_uri is None:
        audio_uri = (
            f"https://source.example.invalid/{dataset_name}/"
            f"{source_group}/{row_index}.{suffix}"
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
            audio_uri="gs://source-bucket/feeds/feeds-a/3.flac",
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
        excluded_rows=(
            ExcludedRow(
                dataset_name="calls",
                row_index=99,
                audio_uri="gs://source-bucket/calls/skipped.flac",
                reason="empty_text",
            ),
        ),
        scratch_dir="/tmp/sft-audio",
        audio_preparer=_fake_audio_preparer,
        audio_uploader=_fake_audio_uploader,
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
        f"{root}/reports/excluded_rows.jsonl",
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


def _jsonl_rows(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _fake_audio_preparer(
    storage_client: FakeStorageClient,
    *,
    layout: object,
    segments: tuple[LabeledSegment, ...],
    scratch_dir: str | Path | None = None,
    upload: bool = True,
) -> AudioPreparationResult:
    storage_client.events.append(f"prepare_audio:{scratch_dir}:upload={upload}")
    enriched_segments = tuple(
        _enriched_segment(segment) for segment in segments
    )
    upload_tasks = tuple(
        AudioUploadTask(
            uri=str(segment.model_ready_audio_uri),
            local_path=Path(f"/tmp/fake-audio/{segment.row_index}.flac"),
            content_type="audio/flac",
        )
        for segment in enriched_segments
        if (segment.transformation_metadata or {}).get("action") != "reused"
    )
    return AudioPreparationResult(
        segments=enriched_segments,
        plans=(),
        uploaded_audio_uris=tuple(task.uri for task in upload_tasks)
        if upload
        else (),
        action_counts={
            "reused": 1,
            "copied": 1,
            "derived": 1,
            "transcoded": 0,
        },
        upload_tasks=upload_tasks,
    )


def _fake_audio_uploader(
    storage_client: FakeStorageClient,
    audio_result: AudioPreparationResult,
) -> tuple[str, ...]:
    storage_client.events.append("upload_audio")
    return tuple(task.uri for task in audio_result.upload_tasks)


def _enriched_segment(segment: LabeledSegment) -> LabeledSegment:
    action_by_row = {
        1: "derived",
        2: "copied",
        3: "reused",
    }
    action = action_by_row[segment.row_index]
    if action == "reused":
        model_ready_audio_uri = segment.audio_uri
    else:
        model_ready_audio_uri = (
            f"gs://wd-ready/{segment.dataset_name}/"
            f"{segment.source_group}/{segment.row_index}.flac"
        )
    return replace(
        segment,
        model_ready_audio_uri=model_ready_audio_uri,
        derived_audio_uri=(
            model_ready_audio_uri if action == "derived" else None
        ),
        transformation_metadata={
            "action": action,
            "original_audio_uri": segment.original_audio_uri,
            "source_audio_uri": segment.audio_uri,
            "offset": segment.offset,
            "duration": segment.duration,
            "source_duration": segment.duration + 1.0,
            "output_duration": segment.duration,
            "source_format": "mp3",
            "output_format": "flac",
            "source_channels": 2,
            "output_channels": 1,
            "mixed_to_mono": action in {"derived", "transcoded"},
            "resampled": False,
            "padded": False,
            "split": segment.split,
            "source_group": segment.source_group,
        },
    )


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
            result.artifact_inventory["audio_action_prefixes"],
            {
                "copied": "gs://wd-transcription-data/sft/dv-001/audio/copied/",
                "derived": "gs://wd-transcription-data/sft/dv-001/audio/derived/",
                "transcoded": "gs://wd-transcription-data/sft/dv-001/audio/transcoded/",
            },
        )
        self.assertEqual(
            result.uploaded_audio_uris,
            (
                "gs://wd-ready/calls/calls-a/1.flac",
                "gs://wd-ready/calls/calls-b/2.flac",
            ),
        )
        self.assertEqual(
            result.audio_action_counts,
            {
                "reused": 1,
                "copied": 1,
                "derived": 1,
                "transcoded": 0,
            },
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
        excluded_upload = _upload_by_uri(client)[
            "gs://wd-transcription-data/sft/dv-001/reports/excluded_rows.jsonl"
        ]
        self.assertEqual(
            excluded_upload["content_type"], "application/x-ndjson"
        )
        self.assertEqual(
            _jsonl_rows(excluded_upload["text"]),
            [
                {
                    "dataset_name": "calls",
                    "row_index": 99,
                    "audio_uri": "gs://source-bucket/calls/skipped.flac",
                    "reason": "empty_text",
                }
            ],
        )

    def test_publish_prepares_audio_before_text_artifacts(self) -> None:
        client = FakeStorageClient()

        _publish(client)

        self.assertEqual(client.events[0], "list:sft/dv-001/")
        prepare_index = client.events.index(
            "prepare_audio:/tmp/sft-audio:upload=False"
        )
        audio_upload_index = client.events.index("upload_audio")
        first_text_upload_index = next(
            index
            for index, event in enumerate(client.events)
            if event.startswith("upload_text:")
        )
        self.assertLess(prepare_index, first_text_upload_index)
        self.assertLess(prepare_index, audio_upload_index)
        self.assertLess(audio_upload_index, first_text_upload_index)

    def test_publish_checks_dataset_version_absent_once(self) -> None:
        client = FakeStorageClient()

        _publish(client)

        self.assertEqual(len(client.list_calls), 1)
        self.assertEqual(
            client.events.count("list:sft/dv-001/"),
            1,
        )
        self.assertLess(
            client.events.index("list:sft/dv-001/"),
            client.events.index("prepare_audio:/tmp/sft-audio:upload=False"),
        )

    def test_publish_uses_enriched_segments_for_model_writers(self) -> None:
        client = FakeStorageClient()

        _publish(client)

        uploads = _upload_by_uri(client)
        nemo_rows = _jsonl_rows(
            uploads[
                "gs://wd-transcription-data/sft/dv-001/model_inputs/nemo/train.jsonl"
            ]["text"]
        )
        whisper_rows = _jsonl_rows(
            uploads[
                "gs://wd-transcription-data/sft/dv-001/model_inputs/whisper/train.jsonl"
            ]["text"]
        )
        gemini_rows = _jsonl_rows(
            uploads[
                "gs://wd-transcription-data/sft/dv-001/model_inputs/gemini/train.jsonl"
            ]["text"]
        )
        canonical_rows = _jsonl_rows(
            uploads[
                "gs://wd-transcription-data/sft/dv-001/manifests/canonical/train.jsonl"
            ]["text"]
        )

        self.assertEqual(
            {row["audio_filepath"] for row in nemo_rows},
            {
                "gs://wd-ready/calls/calls-a/1.flac",
                "gs://source-bucket/feeds/feeds-a/3.flac",
            },
        )
        self.assertEqual(
            {row["audio_uri"] for row in whisper_rows},
            {
                "gs://wd-ready/calls/calls-a/1.flac",
                "gs://source-bucket/feeds/feeds-a/3.flac",
            },
        )
        self.assertEqual(
            {
                row["contents"][0]["parts"][0]["fileData"]["fileUri"]
                for row in gemini_rows
            },
            {
                "gs://wd-ready/calls/calls-a/1.flac",
                "gs://source-bucket/feeds/feeds-a/3.flac",
            },
        )
        self.assertEqual(
            {row["audio_uri"] for row in canonical_rows},
            {
                "https://source.example.invalid/calls/calls-a/1.flac",
                "gs://source-bucket/feeds/feeds-a/3.flac",
            },
        )
        self.assertEqual(
            {row["model_ready_audio_uri"] for row in canonical_rows},
            {
                "gs://wd-ready/calls/calls-a/1.flac",
                "gs://source-bucket/feeds/feeds-a/3.flac",
            },
        )

    def test_publish_does_not_accept_force_resume_or_cleanup_arguments(
        self,
    ) -> None:
        base_kwargs = {
            "dataset_version_id": "dv-001",
            "segments": _segments(),
            "resolved_config": {"dataset_version_id": "dv-001"},
            "leakage_validation": {"passed": True},
            "balance_report": {"score": 0.92, "components": {}},
            "system_prompt": "You are a radio transcription model.",
            "user_prompt": "Transcribe the emergency radio audio.",
            "audio_preparer": _fake_audio_preparer,
        }
        forbidden_kwargs = (
            "force",
            "overwrite",
            "resume",
            "cleanup",
            "delete",
        )

        for keyword in forbidden_kwargs:
            with self.subTest(keyword=keyword):
                client = FakeStorageClient()
                with self.assertRaises(TypeError):
                    publish_dataset_version_artifacts(
                        client, **base_kwargs, **{keyword: True}
                    )
                self.assertEqual(client.list_calls, [])
                self.assertEqual(client.uploads, [])

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

    def test_publish_does_not_upload_audio_when_text_planning_fails(
        self,
    ) -> None:
        client = FakeStorageClient()

        with self.assertRaises(ValueError):
            publish_dataset_version_artifacts(
                client,
                dataset_version_id="dv-001",
                segments=_segments(),
                resolved_config={
                    "dataset_version_id": "dv-001",
                    "bad_float": math.nan,
                },
                leakage_validation={"passed": True},
                balance_report={"score": 0.92, "components": {}},
                system_prompt="You are a radio transcription model.",
                user_prompt="Transcribe the emergency radio audio.",
                scratch_dir="/tmp/sft-audio",
                audio_preparer=_fake_audio_preparer,
                audio_uploader=_fake_audio_uploader,
            )

        self.assertIn(
            "prepare_audio:/tmp/sft-audio:upload=False", client.events
        )
        self.assertNotIn("upload_audio", client.events)
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
