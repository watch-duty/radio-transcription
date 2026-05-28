from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)

from dataset_split.leakage import (  # noqa: E402
    SplitLeakageError,
    validate_model_ready_audio,
    validate_no_duplicate_audio_spans,
    validate_split_integrity,
    validate_split_leakage,
)
from dataset_split.types import LabeledSegment  # noqa: E402


def _segment(
    source_group: str,
    *,
    split: str | None = "train",
    original_audio_uri: str = "gs://bucket/audio.mp3",
    model_ready_audio_uri: str | None = None,
    text: str = "alpha bravo",
    offset: float = 0.0,
    duration: float = 10.0,
    row_index: int = 0,
    derived_audio_uri: str | None = None,
    transformation_metadata: dict[str, object] | None = None,
) -> LabeledSegment:
    audio_uri = f"gs://bucket/{source_group}/{row_index}.mp3"
    return LabeledSegment(
        dataset_name="calls",
        dataset_family="bcfy_calls",
        source_strategy="bcfy_calls",
        source_group=source_group,
        audio_uri=audio_uri,
        original_audio_uri=original_audio_uri,
        text=text,
        row_index=row_index,
        offset=offset,
        duration=duration,
        split=split,
        model_ready_audio_uri=model_ready_audio_uri,
        derived_audio_uri=derived_audio_uri,
        transformation_metadata=transformation_metadata,
    )


def _transformation_metadata(
    *,
    action: str = "copied",
    split: str = "train",
    source_group: str = "feed-a",
) -> dict[str, object]:
    return {
        "action": action,
        "original_audio_uri": "gs://bucket/original.wav",
        "source_audio_uri": "gs://bucket/source.wav",
        "offset": 0.0,
        "duration": 10.0,
        "source_duration": 10.0,
        "output_duration": 10.0,
        "source_format": "flac",
        "output_format": "flac",
        "source_channels": 1,
        "output_channels": 1,
        "mixed_to_mono": False,
        "resampled": False,
        "padded": False,
        "split": split,
        "source_group": source_group,
    }


class TestSplitLeakage(unittest.TestCase):
    def test_cross_split_source_group_overlap_fails(self) -> None:
        segments = (
            _segment("feed-a", split="train", row_index=0),
            _segment(
                "feed-a",
                split="eval",
                original_audio_uri="gs://bucket/other.mp3",
                row_index=1,
            ),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "source_group appears in both splits"
        ):
            validate_split_leakage(segments)

    def test_cross_split_original_audio_uri_overlap_fails(self) -> None:
        segments = (
            _segment("feed-a", split="train", row_index=0),
            _segment("feed-b", split="eval", row_index=1),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "original_audio_uri appears in both splits"
        ):
            validate_split_leakage(segments)

    def test_cross_split_model_ready_audio_uri_overlap_fails(self) -> None:
        segments = (
            _segment(
                "feed-a",
                split="train",
                original_audio_uri="gs://bucket/a.mp3",
                model_ready_audio_uri="gs://ready/clip.flac",
                row_index=0,
            ),
            _segment(
                "feed-b",
                split="eval",
                original_audio_uri="gs://bucket/b.mp3",
                model_ready_audio_uri="gs://ready/clip.flac",
                row_index=1,
            ),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "model_ready_audio_uri appears in both splits"
        ):
            validate_split_leakage(segments)

    def test_empty_model_ready_audio_uri_is_ignored(self) -> None:
        segments = (
            _segment(
                "feed-a",
                split="train",
                original_audio_uri="gs://bucket/a.mp3",
                model_ready_audio_uri=" ",
                row_index=0,
            ),
            _segment(
                "feed-b",
                split="eval",
                original_audio_uri="gs://bucket/b.mp3",
                model_ready_audio_uri=None,
                row_index=1,
            ),
        )

        validate_split_leakage(segments)

    def test_duplicate_audio_span_in_same_split_fails(self) -> None:
        segments = (
            _segment("feed-a", split="train", row_index=0),
            _segment("feed-b", split="train", row_index=1),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "duplicate audio span in train"
        ):
            validate_no_duplicate_audio_spans(segments)

    def test_duplicate_audio_span_ignores_text(self) -> None:
        segments = (
            _segment("feed-a", split="train", text="alpha", row_index=0),
            _segment("feed-b", split="train", text="bravo", row_index=1),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "duplicate audio span in train"
        ):
            validate_no_duplicate_audio_spans(segments)

    def test_same_original_audio_different_offset_is_allowed(self) -> None:
        segments = (
            _segment("feed-a", split="train", offset=0.0, row_index=0),
            _segment("feed-b", split="train", offset=5.0, row_index=1),
        )

        validate_no_duplicate_audio_spans(segments)

    def test_uri_comparison_strips_whitespace_only(self) -> None:
        segments = (
            _segment(
                "feed-a",
                split="train",
                original_audio_uri=" gs://bucket/audio.mp3 ",
                row_index=0,
            ),
            _segment(
                "feed-b",
                split="eval",
                original_audio_uri="gs://bucket/audio.mp3",
                row_index=1,
            ),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "original_audio_uri appears in both splits"
        ):
            validate_split_leakage(segments)

    def test_missing_split_fails_with_row_index(self) -> None:
        with self.assertRaisesRegex(
            SplitLeakageError, "row_index=0 missing split"
        ):
            validate_split_integrity((_segment("feed-a", split=None),))

    def test_model_ready_audio_uri_is_required(self) -> None:
        segment = _segment(
            "feed-a",
            model_ready_audio_uri=None,
            transformation_metadata=_transformation_metadata(),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "row_index=0.*model_ready_audio_uri"
        ):
            validate_model_ready_audio((segment,))

    def test_model_ready_audio_uri_must_be_gcs(self) -> None:
        segment = _segment(
            "feed-a",
            model_ready_audio_uri="https://example.com/audio.flac",
            transformation_metadata=_transformation_metadata(),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "row_index=0.*model_ready_audio_uri"
        ):
            validate_model_ready_audio((segment,))

    def test_transformation_metadata_action_is_required(self) -> None:
        metadata = _transformation_metadata()
        metadata.pop("action")
        segment = _segment(
            "feed-a",
            model_ready_audio_uri="gs://ready/feed-a.flac",
            transformation_metadata=metadata,
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "row_index=0.*action"
        ):
            validate_model_ready_audio((segment,))

    def test_unknown_transformation_action_fails(self) -> None:
        segment = _segment(
            "feed-a",
            model_ready_audio_uri="gs://ready/feed-a.flac",
            transformation_metadata=_transformation_metadata(
                action="normalized"
            ),
        )

        with self.assertRaisesRegex(
            SplitLeakageError, "row_index=0.*action=normalized"
        ):
            validate_model_ready_audio((segment,))

    def test_derived_audio_uri_required_only_for_derived_action(self) -> None:
        missing_derived_uri = _segment(
            "feed-a",
            model_ready_audio_uri="gs://ready/feed-a.flac",
            transformation_metadata=_transformation_metadata(
                action="derived"
            ),
        )
        with self.assertRaisesRegex(
            SplitLeakageError, "row_index=0.*derived_audio_uri"
        ):
            validate_model_ready_audio((missing_derived_uri,))

        stale_derived_uri = _segment(
            "feed-a",
            model_ready_audio_uri="gs://ready/feed-a.flac",
            derived_audio_uri="gs://ready/feed-a.flac",
            transformation_metadata=_transformation_metadata(
                action="copied"
            ),
        )
        with self.assertRaisesRegex(
            SplitLeakageError, "row_index=0.*derived_audio_uri"
        ):
            validate_model_ready_audio((stale_derived_uri,))

        derived_segment = _segment(
            "feed-a",
            model_ready_audio_uri="gs://ready/feed-a.flac",
            derived_audio_uri="gs://ready/feed-a.flac",
            transformation_metadata=_transformation_metadata(
                action="derived"
            ),
        )
        validate_model_ready_audio((derived_segment,))

    def test_transformation_metadata_requires_all_minimum_keys(self) -> None:
        for key in (
            "original_audio_uri",
            "source_audio_uri",
            "offset",
            "duration",
            "source_duration",
            "output_duration",
            "source_format",
            "output_format",
            "source_channels",
            "output_channels",
            "mixed_to_mono",
            "resampled",
            "padded",
            "split",
            "source_group",
        ):
            with self.subTest(key=key):
                metadata = _transformation_metadata()
                metadata.pop(key)
                segment = _segment(
                    "feed-a",
                    model_ready_audio_uri="gs://ready/feed-a.flac",
                    transformation_metadata=metadata,
                )

                with self.assertRaisesRegex(
                    SplitLeakageError, f"row_index=0.*{key}"
                ):
                    validate_model_ready_audio((segment,))

    def test_model_ready_uri_overlap_still_fails_after_enrichment(self) -> None:
        segments = (
            _segment(
                "feed-a",
                split="train",
                original_audio_uri="gs://bucket/a.wav",
                model_ready_audio_uri="gs://ready/shared.flac",
                transformation_metadata=_transformation_metadata(
                    split="train", source_group="feed-a"
                ),
                row_index=0,
            ),
            _segment(
                "feed-b",
                split="eval",
                original_audio_uri="gs://bucket/b.wav",
                model_ready_audio_uri="gs://ready/shared.flac",
                transformation_metadata=_transformation_metadata(
                    split="eval", source_group="feed-b"
                ),
                row_index=1,
            ),
        )

        validate_model_ready_audio(segments)
        with self.assertRaisesRegex(
            SplitLeakageError, "model_ready_audio_uri appears in both splits"
        ):
            validate_split_integrity(segments)


if __name__ == "__main__":
    unittest.main()
