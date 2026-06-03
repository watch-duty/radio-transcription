"""Tests for Phase 3 Label Studio review package helpers."""

from __future__ import annotations

import json
import unittest


def _ranked_row(
    audio_segment_id: str = "audio-a",
    *,
    rank: int = 1,
    wer: float = 42.5,
    text: str = "Engine 41 copy",
) -> dict[str, object]:
    return {
        "rank": rank,
        "wer": wer,
        "insertions": 1,
        "deletions": 2,
        "substitutions": 3,
        "hits": 4,
        "audio_segment_id": audio_segment_id,
        "source_window_id": f"source-window-{audio_segment_id}",
        "model_ready_audio_uri": f"gs://bucket/{audio_segment_id}.flac",
        "original_audio_uri": "gs://bucket/original.wav",
        "offset": 12.5,
        "duration": 2.5,
        "source_group": "source-a",
        "row_index": rank,
        "split": "train",
        "dataset_name": "test-dataset",
        "text": text,
        "prediction_text": "MODEL PREDICTION MUST NOT LEAK",
        "model_id": "gemini-3.5-flash",
        "prompt_fingerprint": "prompt-fp",
        "context_policy_fingerprint": "context-policy-fp",
        "num_recent_events": 1000,
        "context_fingerprint": "context-fp",
        "cache_created_at": "2026-06-03T00:00:00Z",
    }


class TestTaskGeneration(unittest.TestCase):
    def test_select_ranked_rows_defaults_to_top_1000_in_input_order(
        self,
    ) -> None:
        from common import label_studio_review

        rows = [
            _ranked_row("audio-a", rank=1),
            _ranked_row("audio-b", rank=2),
        ]

        self.assertEqual(label_studio_review.DEFAULT_LIMIT, 1000)
        selected = label_studio_review.select_ranked_rows(rows)
        limited = label_studio_review.select_ranked_rows(rows, limit=1)

        self.assertEqual(
            [row["audio_segment_id"] for row in selected],
            ["audio-a", "audio-b"],
        )
        self.assertEqual(
            [row["audio_segment_id"] for row in limited], ["audio-a"]
        )

    def test_task_data_has_rendered_and_hidden_audit_fields(self) -> None:
        from common import label_studio_review

        row = _ranked_row()
        task = label_studio_review.task_from_ranked_row(row)
        data = task["data"]

        self.assertEqual(data["audio"], row["model_ready_audio_uri"])
        self.assertEqual(data["reference_transcript"], row["text"])
        for field in (
            "audio_segment_id",
            "source_window_id",
            "rank",
            "wer",
            "source_group",
            "row_index",
            "model_id",
            "prompt_fingerprint",
            "context_policy_fingerprint",
            "num_recent_events",
            "context_fingerprint",
            "cache_created_at",
        ):
            self.assertEqual(data[field], row[field])
        self.assertNotIn("prediction_text", data)
        self.assertNotIn(
            "MODEL PREDICTION MUST NOT LEAK",
            json.dumps(task, sort_keys=True),
        )

    def test_missing_required_field_fails_loudly(self) -> None:
        from common import label_studio_review

        for missing_field in ("audio_segment_id", "context_fingerprint"):
            row = _ranked_row()
            del row[missing_field]
            with self.subTest(missing_field=missing_field):
                with self.assertRaisesRegex(ValueError, missing_field):
                    label_studio_review.task_from_ranked_row(row)


class TestLabelConfig(unittest.TestCase):
    def test_label_config_contains_only_required_controls(self) -> None:
        from common import label_studio_review

        xml = label_studio_review.build_label_config_xml()

        self.assertIn('<Audio name="audio" value="$audio"', xml)
        self.assertIn(
            '<Text name="reference_transcript" value="$reference_transcript"',
            xml,
        )
        self.assertIn(
            '<TextArea name="transcription" toName="audio" '
            'value="$reference_transcript"',
            xml,
        )
        self.assertIn('editable="true"', xml)
        self.assertIn('transcription="true"', xml)
        self.assertIn(
            '<Choices name="review_status" toName="audio" '
            'choice="single" required="true"',
            xml,
        )
        self.assertIn('<Choice value="Reviewed"', xml)
        self.assertIn('<Choice value="Skip"', xml)
        textarea_tag = xml.split('<TextArea name="transcription"', 1)[1].split(
            "/>",
            1,
        )[0]
        self.assertNotIn('required="true"', textarea_tag)


class TestPackageArtifacts(unittest.TestCase):
    def test_preview_and_readme_omit_prediction_text(self) -> None:
        from common import label_studio_review

        preview = label_studio_review.preview_row_from_ranked_row(_ranked_row())
        readme = label_studio_review.render_operator_readme()

        self.assertEqual(preview["reference_transcript"], "Engine 41 copy")
        self.assertEqual(preview["audio_segment_id"], "audio-a")
        self.assertEqual(preview["source_window_id"], "source-window-audio-a")
        self.assertEqual(preview["source_group"], "source-a")
        self.assertNotIn("prediction_text", preview)
        self.assertNotIn("MODEL PREDICTION MUST NOT LEAK", readme)
        self.assertIn("roles/storage.objectViewer", readme)
        self.assertIn(
            "gcloud storage buckets add-iam-policy-binding "
            "gs://wd-transcription-data",
            readme,
        )
        self.assertIn("serviceAccount:LABEL_STUDIO_SERVICE_ACCOUNT", readme)
        self.assertIn("GCS source storage", readme)
        self.assertIn("Reviewed", readme)
        self.assertIn("Skip", readme)

    def test_build_package_returns_all_artifact_content(self) -> None:
        from common import label_studio_review

        package = label_studio_review.build_package(
            [_ranked_row("audio-a"), _ranked_row("audio-b", rank=2)],
            limit=1,
        )

        self.assertEqual(len(package.tasks), 1)
        self.assertEqual(len(package.preview_rows), 1)
        self.assertIn("review_status", package.label_config_xml)
        self.assertIn("GCS source storage", package.readme_text)
        self.assertNotIn(
            "MODEL PREDICTION MUST NOT LEAK",
            json.dumps(package.tasks, sort_keys=True),
        )


if __name__ == "__main__":
    unittest.main()
