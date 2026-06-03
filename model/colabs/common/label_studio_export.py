"""Parse raw Label Studio exports into reviewed transcript fact rows."""

from __future__ import annotations

import collections.abc
import dataclasses

from common import label_studio_review


TRANSCRIPTION_FROM_NAME = "transcription"
REVIEW_STATUS_FROM_NAME = "review_status"
REVIEWED_STATUS = "Reviewed"
SKIP_STATUS = "Skip"

REVIEWED_OUTPUT_FIELDS = (
    "task_id",
    "annotation_id",
    "annotation_completed_at",
    "annotation_updated_at",
    "annotation_completed_by",
    "annotation_lead_time",
    "audio",
    "audio_segment_id",
    "source_window_id",
    "model_ready_audio_uri",
    "original_audio_uri",
    "offset",
    "duration",
    "source_group",
    "row_index",
    "dataset_name",
    "rank",
    "wer",
    "insertions",
    "deletions",
    "substitutions",
    "hits",
    "model_id",
    "prompt_fingerprint",
    "context_policy_fingerprint",
    "num_recent_events",
    "context_fingerprint",
    "cache_created_at",
    "label_studio_review_status",
    "original_reference_transcript",
    "submitted_transcript",
)

ERROR_OUTPUT_FIELDS = (
    "task_id",
    "audio_segment_id",
    "annotation_id",
    "error_code",
    "message",
)

_TASK_DATA_OUTPUT_FIELDS = (
    "audio",
    "audio_segment_id",
    "source_window_id",
    "model_ready_audio_uri",
    "original_audio_uri",
    "offset",
    "duration",
    "source_group",
    "row_index",
    "dataset_name",
    "rank",
    "wer",
    "insertions",
    "deletions",
    "substitutions",
    "hits",
    "model_id",
    "prompt_fingerprint",
    "context_policy_fingerprint",
    "num_recent_events",
    "context_fingerprint",
    "cache_created_at",
)

_REQUIRED_TASK_DATA_FIELDS = (
    "audio",
    "reference_transcript",
    *label_studio_review.AUDIT_FIELDS,
)


@dataclasses.dataclass(frozen=True)
class LabelStudioExportParseResult:
    """Parsed Label Studio export content.

    Attributes:
        reviewed_rows: Reviewed transcript fact rows for downstream overlay.
        error_rows: Structured parse errors for malformed selected exports.
    """

    reviewed_rows: list[dict[str, object]]
    error_rows: list[dict[str, object]]


def parse_export_tasks(
    tasks: collections.abc.Iterable[collections.abc.Mapping[str, object]],
) -> LabelStudioExportParseResult:
    """Parse raw Label Studio export tasks into reviewed fact rows.

    Args:
        tasks: Raw Label Studio task dictionaries from a JSON export.

    Returns:
        Parsed reviewed rows and structured error rows.
    """
    reviewed_rows = []
    error_rows = []
    for task in tasks:
        row, error = _parse_task(task)
        if error is not None:
            error_rows.append(error)
            continue
        if row is not None:
            reviewed_rows.append(row)
    return LabelStudioExportParseResult(
        reviewed_rows=reviewed_rows,
        error_rows=error_rows,
    )


def _parse_task(
    task: collections.abc.Mapping[str, object],
) -> tuple[dict[str, object] | None, dict[str, object] | None]:
    for annotation in _completed_annotations(task):
        review_status, status_error = _parse_review_status(task, annotation)
        if status_error is not None:
            return None, status_error
        if review_status == SKIP_STATUS:
            continue
        if review_status != REVIEWED_STATUS:
            continue
        transcript, transcript_error = _parse_transcript(task, annotation)
        if transcript_error is not None:
            return None, transcript_error
        data, data_error = _validated_task_data(task, annotation)
        if data_error is not None:
            return None, data_error
        return _reviewed_row(task, annotation, data, transcript), None
    return None, None


def _completed_annotations(
    task: collections.abc.Mapping[str, object],
) -> list[collections.abc.Mapping[str, object]]:
    annotations = task.get("annotations", [])
    if not isinstance(annotations, list):
        return []
    completed = []
    for export_order, annotation in enumerate(annotations):
        if not isinstance(annotation, collections.abc.Mapping):
            continue
        if annotation.get("was_cancelled") is True:
            continue
        if not _is_completed_annotation(annotation):
            continue
        completed.append((annotation, export_order))
    completed.sort(
        key=lambda item: _annotation_sort_key(item[0], item[1]),
        reverse=True,
    )
    return [annotation for annotation, _ in completed]


def _is_completed_annotation(
    annotation: collections.abc.Mapping[str, object],
) -> bool:
    if annotation.get("completed_at"):
        return True
    return annotation.get("last_action") == "submitted"


def _annotation_sort_key(
    annotation: collections.abc.Mapping[str, object],
    export_order: int,
) -> tuple[str, str, tuple[int, object], int]:
    updated_at = _string_value(annotation.get("updated_at"))
    completed_at = _string_value(annotation.get("completed_at")) or updated_at
    return (
        completed_at,
        updated_at,
        _annotation_id_sort_key(annotation.get("id")),
        export_order,
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


def _parse_review_status(
    task: collections.abc.Mapping[str, object],
    annotation: collections.abc.Mapping[str, object],
) -> tuple[str | None, dict[str, object] | None]:
    results = _results_for_from_name(annotation, REVIEW_STATUS_FROM_NAME)
    if not results:
        return None, _error_row(
            task,
            annotation,
            "missing_review_status",
            "annotation missing review_status result",
        )
    if len(results) > 1:
        return None, _error_row(
            task,
            annotation,
            "duplicate_review_status",
            "annotation has duplicate review_status results",
        )
    value = results[0].get("value")
    if not isinstance(value, collections.abc.Mapping):
        return None, _unexpected_review_status_error(task, annotation)
    choices = value.get("choices")
    if not isinstance(choices, list) or len(choices) != 1:
        return None, _unexpected_review_status_error(task, annotation)
    choice = choices[0]
    if choice not in (REVIEWED_STATUS, SKIP_STATUS):
        return None, _unexpected_review_status_error(task, annotation)
    return str(choice), None


def _unexpected_review_status_error(
    task: collections.abc.Mapping[str, object],
    annotation: collections.abc.Mapping[str, object],
) -> dict[str, object]:
    return _error_row(
        task,
        annotation,
        "unexpected_review_status",
        "annotation review_status must be Reviewed or Skip",
    )


def _parse_transcript(
    task: collections.abc.Mapping[str, object],
    annotation: collections.abc.Mapping[str, object],
) -> tuple[str | None, dict[str, object] | None]:
    results = _results_for_from_name(annotation, TRANSCRIPTION_FROM_NAME)
    if not results:
        return None, _error_row(
            task,
            annotation,
            "missing_transcription",
            "Reviewed annotation missing transcription result",
        )
    if len(results) > 1:
        return None, _error_row(
            task,
            annotation,
            "duplicate_transcription",
            "Reviewed annotation has duplicate transcription results",
        )
    value = results[0].get("value")
    if not isinstance(value, collections.abc.Mapping):
        return None, _invalid_transcription_error(task, annotation)
    text_values = value.get("text")
    if text_values is None:
        return "", None
    if not isinstance(text_values, list):
        return None, _invalid_transcription_error(task, annotation)
    if not text_values:
        return "", None
    if len(text_values) > 1:
        return None, _invalid_transcription_error(task, annotation)
    text = text_values[0]
    if not isinstance(text, str):
        return None, _invalid_transcription_error(task, annotation)
    return text.strip(), None


def _invalid_transcription_error(
    task: collections.abc.Mapping[str, object],
    annotation: collections.abc.Mapping[str, object],
) -> dict[str, object]:
    return _error_row(
        task,
        annotation,
        "invalid_transcription_value",
        "Reviewed annotation transcription value must contain zero or one text",
    )


def _results_for_from_name(
    annotation: collections.abc.Mapping[str, object],
    from_name: str,
) -> list[collections.abc.Mapping[str, object]]:
    results = annotation.get("result", [])
    if not isinstance(results, list):
        return []
    return [
        result
        for result in results
        if (
            isinstance(result, collections.abc.Mapping)
            and result.get("from_name") == from_name
        )
    ]


def _validated_task_data(
    task: collections.abc.Mapping[str, object],
    annotation: collections.abc.Mapping[str, object],
) -> tuple[
    collections.abc.Mapping[str, object] | None, dict[str, object] | None
]:
    data = task.get("data")
    if not isinstance(data, collections.abc.Mapping):
        return None, _error_row(
            task,
            annotation,
            "missing_task_data_field",
            "task missing data object",
        )
    for field in _REQUIRED_TASK_DATA_FIELDS:
        if field not in data or data[field] is None:
            return None, _error_row(
                task,
                annotation,
                "missing_task_data_field",
                f"task data missing required field: {field}",
            )
    return data, None


def _reviewed_row(
    task: collections.abc.Mapping[str, object],
    annotation: collections.abc.Mapping[str, object],
    data: collections.abc.Mapping[str, object],
    submitted_transcript: str | None,
) -> dict[str, object]:
    completed_at = annotation.get("completed_at") or annotation.get(
        "updated_at"
    )
    row = {
        "task_id": task.get("id"),
        "annotation_id": annotation.get("id"),
        "annotation_completed_at": completed_at,
        "annotation_updated_at": annotation.get("updated_at"),
        "annotation_completed_by": annotation.get("completed_by"),
        "annotation_lead_time": annotation.get("lead_time"),
    }
    for field in _TASK_DATA_OUTPUT_FIELDS:
        row[field] = data[field]
    row["label_studio_review_status"] = REVIEWED_STATUS
    row["original_reference_transcript"] = data["reference_transcript"]
    row["submitted_transcript"] = submitted_transcript
    return {field: row.get(field) for field in REVIEWED_OUTPUT_FIELDS}


def _error_row(
    task: collections.abc.Mapping[str, object],
    annotation: collections.abc.Mapping[str, object] | None,
    error_code: str,
    message: str,
) -> dict[str, object]:
    data = task.get("data")
    audio_segment_id = None
    if isinstance(data, collections.abc.Mapping):
        audio_segment_id = data.get("audio_segment_id")
    row = {
        "task_id": task.get("id"),
        "audio_segment_id": audio_segment_id,
        "annotation_id": None if annotation is None else annotation.get("id"),
        "error_code": error_code,
        "message": message,
    }
    return {field: row.get(field) for field in ERROR_OUTPUT_FIELDS}


def _string_value(value: object) -> str:
    if isinstance(value, str):
        return value
    return ""
