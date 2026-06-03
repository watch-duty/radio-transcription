"""Build latest-only transcript correction overlay rows."""

from __future__ import annotations

import collections.abc
import dataclasses

from common import label_studio_export


OVERLAY_ACTION_REPLACE_REFERENCE = "replace_reference"
OVERLAY_ACTION_EXCLUDE = "exclude"

OVERLAY_STATUS_REVIEWED_EDITED = "reviewed_edited"
OVERLAY_STATUS_REVIEWED_UNCHANGED = "reviewed_unchanged"
OVERLAY_STATUS_REVIEWED_EMPTY = "reviewed_empty"

OVERLAY_OUTPUT_FIELDS = (
    *label_studio_export.REVIEWED_OUTPUT_FIELDS,
    "overlay_status",
    "overlay_action",
    "replacement_transcript",
)

SUMMARY_FIELDS = (
    "input_row_count",
    "overlay_row_count",
    "reviewed_total",
    "reviewed_edited",
    "reviewed_unchanged",
    "reviewed_empty",
    "duplicate_superseded_count",
    "malformed_error_count",
    "matching_key",
    "source_window_fallback_matching",
    "overlay_actions",
)

ERROR_OUTPUT_FIELDS = (
    "input_index",
    "audio_segment_id",
    "annotation_id",
    "error_code",
    "message",
)

_REQUIRED_FIELDS = (
    "audio_segment_id",
    "source_window_id",
    "label_studio_review_status",
    "original_reference_transcript",
    "submitted_transcript",
    "annotation_id",
    "annotation_completed_at",
    "annotation_updated_at",
)


@dataclasses.dataclass(frozen=True)
class CorrectionOverlayBuildResult:
    """Latest correction overlay rows plus summary and validation errors.

    Attributes:
        overlay_rows: Latest-only overlay rows keyed by `audio_segment_id`.
        summary: Compact count and policy metadata for the build.
        error_rows: Structured validation errors for malformed reviewed rows.
    """

    overlay_rows: list[dict[str, object]]
    summary: dict[str, object]
    error_rows: list[dict[str, object]]


def build_latest_overlay(
    reviewed_rows: collections.abc.Iterable[
        collections.abc.Mapping[str, object]
    ],
) -> CorrectionOverlayBuildResult:
    """Build latest-only correction overlay rows from Phase 4 reviewed facts.

    Args:
        reviewed_rows: Phase 4 reviewed transcript fact rows.

    Returns:
        Overlay rows, summary counts, and structured validation errors. If any
        validation error exists, no consumable overlay rows are returned.
    """
    rows = list(reviewed_rows)
    error_rows = _validate_rows(rows)
    if error_rows:
        return CorrectionOverlayBuildResult(
            overlay_rows=[],
            summary=_summary(
                input_row_count=len(rows),
                overlay_rows=[],
                duplicate_superseded_count=0,
                malformed_error_count=len(error_rows),
            ),
            error_rows=error_rows,
        )

    latest_by_audio_segment_id: dict[str, tuple[dict[str, object], int]] = {}
    first_seen_audio_segment_ids: list[str] = []
    duplicate_superseded_count = 0
    for input_index, row in enumerate(rows):
        audio_segment_id = str(row["audio_segment_id"])
        row_with_policy = _overlay_row(row)
        current = latest_by_audio_segment_id.get(audio_segment_id)
        if current is None:
            first_seen_audio_segment_ids.append(audio_segment_id)
            latest_by_audio_segment_id[audio_segment_id] = (
                row_with_policy,
                input_index,
            )
            continue
        duplicate_superseded_count += 1
        current_row, current_index = current
        if _latest_sort_key(row_with_policy, input_index) > _latest_sort_key(
            current_row,
            current_index,
        ):
            latest_by_audio_segment_id[audio_segment_id] = (
                row_with_policy,
                input_index,
            )

    overlay_rows = [
        latest_by_audio_segment_id[audio_segment_id][0]
        for audio_segment_id in first_seen_audio_segment_ids
    ]
    return CorrectionOverlayBuildResult(
        overlay_rows=overlay_rows,
        summary=_summary(
            input_row_count=len(rows),
            overlay_rows=overlay_rows,
            duplicate_superseded_count=duplicate_superseded_count,
            malformed_error_count=0,
        ),
        error_rows=[],
    )


def _validate_rows(
    rows: collections.abc.Sequence[collections.abc.Mapping[str, object]],
) -> list[dict[str, object]]:
    error_rows = []
    for input_index, row in enumerate(rows):
        for field in _REQUIRED_FIELDS:
            if field not in row or row[field] is None:
                error_rows.append(
                    _error_row(
                        row,
                        input_index,
                        "missing_required_field",
                        f"reviewed row missing required field: {field}",
                    )
                )
                break
            if field != "submitted_transcript" and row[field] == "":
                error_rows.append(
                    _error_row(
                        row,
                        input_index,
                        "missing_required_field",
                        f"reviewed row missing required field: {field}",
                    )
                )
                break
        else:
            if row["label_studio_review_status"] != (
                label_studio_export.REVIEWED_STATUS
            ):
                error_rows.append(
                    _error_row(
                        row,
                        input_index,
                        "unexpected_review_status",
                        "reviewed row status must be Reviewed",
                    )
                )
    return error_rows


def _overlay_row(
    row: collections.abc.Mapping[str, object],
) -> dict[str, object]:
    submitted_transcript = str(row["submitted_transcript"])
    original_reference_transcript = str(row["original_reference_transcript"])
    if not submitted_transcript.strip():
        overlay_status = OVERLAY_STATUS_REVIEWED_EMPTY
        overlay_action = OVERLAY_ACTION_EXCLUDE
        replacement_transcript = ""
    elif submitted_transcript == original_reference_transcript:
        overlay_status = OVERLAY_STATUS_REVIEWED_UNCHANGED
        overlay_action = OVERLAY_ACTION_REPLACE_REFERENCE
        replacement_transcript = submitted_transcript
    else:
        overlay_status = OVERLAY_STATUS_REVIEWED_EDITED
        overlay_action = OVERLAY_ACTION_REPLACE_REFERENCE
        replacement_transcript = submitted_transcript

    values = {field: row.get(field) for field in OVERLAY_OUTPUT_FIELDS}
    values["overlay_status"] = overlay_status
    values["overlay_action"] = overlay_action
    values["replacement_transcript"] = replacement_transcript
    return values


def _latest_sort_key(
    row: collections.abc.Mapping[str, object],
    input_index: int,
) -> tuple[str, str, tuple[int, object], int]:
    return (
        str(row["annotation_completed_at"]),
        str(row["annotation_updated_at"]),
        _annotation_id_sort_key(row["annotation_id"]),
        input_index,
    )


def _annotation_id_sort_key(value: object) -> tuple[int, object]:
    if isinstance(value, int):
        return (1, value)
    if isinstance(value, str):
        try:
            return (1, int(value))
        except ValueError:
            return (0, value)
    return (0, "")


def _summary(
    *,
    input_row_count: int,
    overlay_rows: collections.abc.Sequence[
        collections.abc.Mapping[str, object]
    ],
    duplicate_superseded_count: int,
    malformed_error_count: int,
) -> dict[str, object]:
    reviewed_edited = _count_status(
        overlay_rows,
        OVERLAY_STATUS_REVIEWED_EDITED,
    )
    reviewed_unchanged = _count_status(
        overlay_rows,
        OVERLAY_STATUS_REVIEWED_UNCHANGED,
    )
    reviewed_empty = _count_status(
        overlay_rows,
        OVERLAY_STATUS_REVIEWED_EMPTY,
    )
    overlay_actions = {
        OVERLAY_ACTION_REPLACE_REFERENCE: _count_action(
            overlay_rows,
            OVERLAY_ACTION_REPLACE_REFERENCE,
        ),
        OVERLAY_ACTION_EXCLUDE: _count_action(
            overlay_rows,
            OVERLAY_ACTION_EXCLUDE,
        ),
    }
    values = {
        "input_row_count": input_row_count,
        "overlay_row_count": len(overlay_rows),
        "reviewed_total": len(overlay_rows),
        "reviewed_edited": reviewed_edited,
        "reviewed_unchanged": reviewed_unchanged,
        "reviewed_empty": reviewed_empty,
        "duplicate_superseded_count": duplicate_superseded_count,
        "malformed_error_count": malformed_error_count,
        "matching_key": "audio_segment_id",
        "source_window_fallback_matching": False,
        "overlay_actions": overlay_actions,
    }
    return {field: values[field] for field in SUMMARY_FIELDS}


def _count_status(
    rows: collections.abc.Sequence[collections.abc.Mapping[str, object]],
    status: str,
) -> int:
    return sum(1 for row in rows if row.get("overlay_status") == status)


def _count_action(
    rows: collections.abc.Sequence[collections.abc.Mapping[str, object]],
    action: str,
) -> int:
    return sum(1 for row in rows if row.get("overlay_action") == action)


def _error_row(
    row: collections.abc.Mapping[str, object],
    input_index: int,
    error_code: str,
    message: str,
) -> dict[str, object]:
    values = {
        "input_index": input_index,
        "audio_segment_id": row.get("audio_segment_id"),
        "annotation_id": row.get("annotation_id"),
        "error_code": error_code,
        "message": message,
    }
    return {field: values[field] for field in ERROR_OUTPUT_FIELDS}
