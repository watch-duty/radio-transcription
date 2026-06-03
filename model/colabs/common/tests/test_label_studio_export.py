"""Tests for Phase 4 Label Studio export parsing helpers."""

from __future__ import annotations

import json
import unittest


def _task(
    task_id: int = 101,
    *,
    audio_segment_id: str = "audio-a",
    annotations: list[dict[str, object]] | None = None,
    drafts: list[dict[str, object]] | None = None,
    predictions: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "id": task_id,
        "data": {
            "audio": f"gs://bucket/{audio_segment_id}.flac",
            "reference_transcript": "Engine 41 copy",
            "audio_segment_id": audio_segment_id,
            "source_window_id": f"source-window-{audio_segment_id}",
            "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
            "original_audio_uri": "gs://bucket/original.wav",
            "offset": 12.5,
            "duration": 2.5,
            "source_group": "source-a",
            "row_index": task_id,
            "split": "train",
            "dataset_name": "test-dataset",
            "rank": 1,
            "wer": 42.5,
            "insertions": 1,
            "deletions": 2,
            "substitutions": 3,
            "hits": 4,
            "model_id": "gemini-3.5-flash",
            "prompt_fingerprint": "prompt-fp",
            "context_policy_fingerprint": "context-policy-fp",
            "num_recent_events": 1000,
            "context_fingerprint": "context-fp",
            "cache_created_at": "2026-06-03T00:00:00Z",
        },
        "annotations": [] if annotations is None else annotations,
        "drafts": [] if drafts is None else drafts,
        "predictions": [] if predictions is None else predictions,
    }


def _annotation(
    annotation_id: int = 201,
    *,
    completed_at: str | None = "2026-06-03T01:00:00Z",
    updated_at: str | None = "2026-06-03T01:01:00Z",
    last_action: str = "submitted",
    was_cancelled: bool = False,
    result: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    annotation = {
        "id": annotation_id,
        "updated_at": updated_at,
        "last_action": last_action,
        "was_cancelled": was_cancelled,
        "completed_by": 88,
        "lead_time": 3.25,
        "result": [] if result is None else result,
    }
    if completed_at is not None:
        annotation["completed_at"] = completed_at
    return annotation


def _review_status_result(status: str) -> dict[str, object]:
    return {
        "from_name": "review_status",
        "to_name": "audio",
        "type": "choices",
        "value": {"choices": [status]},
    }


def _transcription_result(text: str) -> dict[str, object]:
    return {
        "from_name": "transcription",
        "to_name": "audio",
        "type": "textarea",
        "value": {"text": [text]},
    }


class TestLabelStudioExportParser(unittest.TestCase):
    def test_parse_reviewed_annotation_preserves_raw_facts(self) -> None:
        from common import label_studio_export

        task = _task(
            annotations=[
                _annotation(
                    result=[
                        _review_status_result("Reviewed"),
                        _transcription_result("Corrected transcript"),
                    ],
                )
            ]
        )

        result = label_studio_export.parse_export_tasks([task])

        self.assertEqual(result.error_rows, [])
        self.assertEqual(len(result.reviewed_rows), 1)
        row = result.reviewed_rows[0]
        self.assertEqual(row["label_studio_review_status"], "Reviewed")
        self.assertEqual(row["original_reference_transcript"], "Engine 41 copy")
        self.assertEqual(row["submitted_transcript"], "Corrected transcript")
        self.assertEqual(row["audio_segment_id"], "audio-a")
        self.assertEqual(row["source_window_id"], "source-window-audio-a")
        self.assertEqual(row["rank"], 1)
        self.assertEqual(row["wer"], 42.5)
        self.assertEqual(row["task_id"], 101)
        self.assertEqual(row["annotation_id"], 201)
        self.assertEqual(row["annotation_completed_at"], "2026-06-03T01:00:00Z")
        serialized = json.dumps(row, sort_keys=True)
        self.assertNotIn("review_outcome", serialized)
        self.assertNotIn("transcript_state", serialized)
        self.assertNotIn("exclude_from_future_inputs", serialized)

    def test_newest_reviewed_wins_even_when_newer_skip_exists(self) -> None:
        from common import label_studio_export

        task = _task(
            annotations=[
                _annotation(
                    301,
                    completed_at="2026-06-03T01:00:00Z",
                    updated_at="2026-06-03T01:01:00Z",
                    result=[
                        _review_status_result("Reviewed"),
                        _transcription_result("Older corrected transcript"),
                    ],
                ),
                _annotation(
                    302,
                    completed_at="2026-06-03T02:00:00Z",
                    updated_at="2026-06-03T02:01:00Z",
                    result=[
                        _review_status_result("Skip"),
                        _transcription_result("Skipped transcript"),
                    ],
                ),
            ]
        )

        result = label_studio_export.parse_export_tasks([task])

        self.assertEqual(result.error_rows, [])
        self.assertEqual(len(result.reviewed_rows), 1)
        self.assertEqual(result.reviewed_rows[0]["annotation_id"], 301)
        self.assertEqual(
            result.reviewed_rows[0]["submitted_transcript"],
            "Older corrected transcript",
        )

    def test_omits_tasks_without_completed_reviewed_annotations(self) -> None:
        from common import label_studio_export

        tasks = [
            _task(1),
            _task(
                2,
                annotations=[
                    _annotation(
                        result=[
                            _review_status_result("Skip"),
                            _transcription_result("Skipped transcript"),
                        ]
                    )
                ],
            ),
            _task(
                3,
                annotations=[
                    _annotation(
                        completed_at=None,
                        last_action="updated",
                        result=[
                            _review_status_result("Reviewed"),
                            _transcription_result("Draft transcript"),
                        ],
                    )
                ],
            ),
            _task(
                4,
                drafts=[
                    {
                        "result": [
                            _review_status_result("Reviewed"),
                            _transcription_result("Draft transcript"),
                        ],
                    }
                ],
            ),
            _task(
                5,
                predictions=[
                    {
                        "result": [
                            _review_status_result("Reviewed"),
                            _transcription_result("Prediction transcript"),
                        ],
                    }
                ],
            ),
            _task(
                6,
                annotations=[
                    _annotation(
                        was_cancelled=True,
                        result=[
                            _review_status_result("Reviewed"),
                            _transcription_result("Cancelled transcript"),
                        ],
                    )
                ],
            ),
        ]

        result = label_studio_export.parse_export_tasks(tasks)

        self.assertEqual(result.reviewed_rows, [])
        self.assertEqual(result.error_rows, [])

    def test_malformed_newest_reviewed_reports_error(self) -> None:
        from common import label_studio_export

        task = _task(
            annotations=[
                _annotation(
                    401,
                    completed_at="2026-06-03T01:00:00Z",
                    updated_at="2026-06-03T01:01:00Z",
                    result=[
                        _review_status_result("Reviewed"),
                        _transcription_result("Older valid transcript"),
                    ],
                ),
                _annotation(
                    402,
                    completed_at="2026-06-03T02:00:00Z",
                    updated_at="2026-06-03T02:01:00Z",
                    result=[
                        _review_status_result("Reviewed"),
                        _transcription_result("Duplicate one"),
                        _transcription_result("Duplicate two"),
                    ],
                ),
            ]
        )

        result = label_studio_export.parse_export_tasks([task])

        self.assertEqual(result.reviewed_rows, [])
        self.assertEqual(len(result.error_rows), 1)
        error = result.error_rows[0]
        self.assertEqual(error["error_code"], "duplicate_transcription")
        self.assertEqual(error["task_id"], 101)
        self.assertEqual(error["audio_segment_id"], "audio-a")
        self.assertEqual(error["annotation_id"], 402)

    def test_submitted_annotation_without_completed_at_uses_updated_at(
        self,
    ) -> None:
        from common import label_studio_export

        task = _task(
            annotations=[
                _annotation(
                    completed_at=None,
                    updated_at="2026-06-03T03:01:00Z",
                    last_action="submitted",
                    result=[
                        _review_status_result("Reviewed"),
                        _transcription_result("Submitted transcript"),
                    ],
                )
            ]
        )

        result = label_studio_export.parse_export_tasks([task])

        self.assertEqual(result.error_rows, [])
        self.assertEqual(len(result.reviewed_rows), 1)
        self.assertEqual(
            result.reviewed_rows[0]["annotation_completed_at"],
            "2026-06-03T03:01:00Z",
        )
        self.assertEqual(
            result.reviewed_rows[0]["annotation_updated_at"],
            "2026-06-03T03:01:00Z",
        )


if __name__ == "__main__":
    unittest.main()
