"""Tests for latest correction overlay construction."""

from __future__ import annotations

import json
import unittest


def _reviewed_row(
    audio_segment_id: str = "audio-a",
    *,
    annotation_id: int = 201,
    annotation_completed_at: str = "2026-06-03T01:00:00Z",
    annotation_updated_at: str = "2026-06-03T01:01:00Z",
    original_reference_transcript: str = "Engine 41 copy",
    submitted_transcript: str = "Engine 41 copies",
    source_window_id: str | None = None,
) -> dict[str, object]:
    if source_window_id is None:
        source_window_id = f"source-window-{audio_segment_id}"
    return {
        "task_id": 101,
        "annotation_id": annotation_id,
        "annotation_completed_at": annotation_completed_at,
        "annotation_updated_at": annotation_updated_at,
        "annotation_completed_by": 88,
        "annotation_lead_time": 3.25,
        "audio": f"gs://bucket/{audio_segment_id}.flac",
        "audio_segment_id": audio_segment_id,
        "source_window_id": source_window_id,
        "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": 12.5,
        "duration": 2.5,
        "source_group": "source-a",
        "row_index": 101,
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
        "label_studio_review_status": "Reviewed",
        "original_reference_transcript": original_reference_transcript,
        "submitted_transcript": submitted_transcript,
    }


class TestCorrectionOverlay(unittest.TestCase):
    def test_build_latest_overlay_encodes_edited_unchanged_and_empty_policy(
        self,
    ) -> None:
        from common import correction_overlay

        rows = [
            _reviewed_row(
                "audio-edited",
                original_reference_transcript="Engine 41 copy",
                submitted_transcript="Engine 41 copies",
            ),
            _reviewed_row(
                "audio-unchanged",
                original_reference_transcript="Medic 2 en route",
                submitted_transcript="Medic 2 en route",
            ),
            _reviewed_row(
                "audio-empty",
                original_reference_transcript="Ignore bad audio",
                submitted_transcript="   ",
            ),
        ]

        result = correction_overlay.build_latest_overlay(rows)

        self.assertEqual(result.error_rows, [])
        rows_by_id = {
            row["audio_segment_id"]: row for row in result.overlay_rows
        }
        self.assertEqual(
            rows_by_id["audio-edited"]["overlay_status"],
            "reviewed_edited",
        )
        self.assertEqual(
            rows_by_id["audio-edited"]["overlay_action"],
            "replace_reference",
        )
        self.assertEqual(
            rows_by_id["audio-edited"]["replacement_transcript"],
            "Engine 41 copies",
        )
        self.assertEqual(
            rows_by_id["audio-unchanged"]["overlay_status"],
            "reviewed_unchanged",
        )
        self.assertEqual(
            rows_by_id["audio-unchanged"]["overlay_action"],
            "replace_reference",
        )
        self.assertEqual(
            rows_by_id["audio-unchanged"]["replacement_transcript"],
            "Medic 2 en route",
        )
        self.assertEqual(
            rows_by_id["audio-empty"]["overlay_status"],
            "reviewed_empty",
        )
        self.assertEqual(
            rows_by_id["audio-empty"]["overlay_action"],
            "exclude",
        )
        self.assertEqual(rows_by_id["audio-empty"]["replacement_transcript"], "")
        self.assertEqual(result.summary["reviewed_edited"], 1)
        self.assertEqual(result.summary["reviewed_unchanged"], 1)
        self.assertEqual(result.summary["reviewed_empty"], 1)

        serialized = json.dumps(result.overlay_rows, sort_keys=True)
        self.assertNotIn("review_history", serialized)
        self.assertNotIn("review_rows", serialized)
        self.assertNotIn("annotation_history", serialized)
        helper_names = dir(correction_overlay)
        self.assertNotIn("apply_manifest", helper_names)

    def test_latest_review_wins_by_audio_segment_id(self) -> None:
        from common import correction_overlay

        result = correction_overlay.build_latest_overlay(
            [
                _reviewed_row(
                    "audio-a",
                    annotation_id=201,
                    annotation_completed_at="2026-06-03T01:00:00Z",
                    annotation_updated_at="2026-06-03T01:01:00Z",
                    submitted_transcript="Older transcript",
                ),
                _reviewed_row(
                    "audio-a",
                    annotation_id=202,
                    annotation_completed_at="2026-06-03T02:00:00Z",
                    annotation_updated_at="2026-06-03T02:01:00Z",
                    submitted_transcript="Newer transcript",
                ),
            ]
        )

        self.assertEqual(result.error_rows, [])
        self.assertEqual(len(result.overlay_rows), 1)
        self.assertEqual(result.overlay_rows[0]["annotation_id"], 202)
        self.assertEqual(
            result.overlay_rows[0]["replacement_transcript"],
            "Newer transcript",
        )
        self.assertEqual(result.summary["duplicate_superseded_count"], 1)

    def test_source_window_id_is_provenance_only(self) -> None:
        from common import correction_overlay

        result = correction_overlay.build_latest_overlay(
            [
                _reviewed_row(
                    "audio-a",
                    source_window_id="source-window-shared",
                ),
                _reviewed_row(
                    "audio-b",
                    source_window_id="source-window-shared",
                ),
            ]
        )

        self.assertEqual(result.error_rows, [])
        self.assertEqual(len(result.overlay_rows), 2)
        self.assertEqual(
            {row["audio_segment_id"] for row in result.overlay_rows},
            {"audio-a", "audio-b"},
        )
        self.assertEqual(
            {row["source_window_id"] for row in result.overlay_rows},
            {"source-window-shared"},
        )

    def test_missing_required_field_returns_error_and_no_overlay(self) -> None:
        from common import correction_overlay

        row = _reviewed_row()
        del row["submitted_transcript"]

        result = correction_overlay.build_latest_overlay([row])

        self.assertEqual(result.overlay_rows, [])
        self.assertEqual(len(result.error_rows), 1)
        self.assertEqual(
            result.error_rows[0]["error_code"],
            "missing_required_field",
        )
        self.assertEqual(result.summary["malformed_error_count"], 1)


if __name__ == "__main__":
    unittest.main()
