"""Label Studio review package helpers for transcript correction workflows."""

from __future__ import annotations

import collections.abc
import dataclasses


DEFAULT_LIMIT = 1000

REQUIRED_RANKED_FIELDS = (
    "rank",
    "wer",
    "insertions",
    "deletions",
    "substitutions",
    "hits",
    "audio_segment_id",
    "source_window_id",
    "model_ready_audio_uri",
    "original_audio_uri",
    "offset",
    "duration",
    "source_group",
    "row_index",
    "split",
    "dataset_name",
    "text",
    "model_id",
    "prompt_fingerprint",
    "context_policy_fingerprint",
    "num_recent_events",
    "context_fingerprint",
    "cache_created_at",
)

AUDIT_FIELDS = tuple(
    field for field in REQUIRED_RANKED_FIELDS if field != "text"
)

PREVIEW_CSV_FIELDS = (
    "rank",
    "wer",
    "model_ready_audio_uri",
    "reference_transcript",
    "audio_segment_id",
    "source_window_id",
    "original_audio_uri",
    "offset",
    "duration",
    "source_group",
    "row_index",
    "split",
    "dataset_name",
    "model_id",
    "prompt_fingerprint",
    "context_policy_fingerprint",
    "num_recent_events",
    "context_fingerprint",
    "cache_created_at",
)


@dataclasses.dataclass(frozen=True)
class LabelStudioPackage:
    """Generated Label Studio review package content.

    Attributes:
        tasks: Label Studio import tasks as JSON-serializable dictionaries.
        label_config_xml: Label Studio labeling configuration XML.
        preview_rows: Human-readable preview CSV rows.
        readme_text: Operator instructions for import and private GCS access.
    """

    tasks: list[dict[str, dict[str, object]]]
    label_config_xml: str
    preview_rows: list[dict[str, object]]
    readme_text: str


def select_ranked_rows(
    ranked_rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
    *,
    limit: int = DEFAULT_LIMIT,
) -> list[dict[str, object]]:
    """Return the top-N ranked rows in their existing order.

    Args:
        ranked_rows: Phase 2 rows already sorted by descending WER.
        limit: Maximum number of rows to return.

    Returns:
        A list of copied row dictionaries.

    Raises:
        ValueError: If `limit` is negative.
    """
    if limit < 0:
        raise ValueError("limit must be non-negative")
    return [dict(row) for row in ranked_rows][:limit]


def task_from_ranked_row(
    row: collections.abc.Mapping[str, object],
) -> dict[str, dict[str, object]]:
    """Build one Label Studio task from one ranked row.

    Args:
        row: Phase 2 ranked row with identity, provenance, and audit fields.

    Returns:
        A Label Studio task dictionary with all fields under `data`.

    Raises:
        ValueError: If a required ranked-row field is missing.
    """
    _validate_required_fields(row)
    data = {
        "audio": row["model_ready_audio_uri"],
        "reference_transcript": row["text"],
    }
    for field in AUDIT_FIELDS:
        data[field] = row[field]
    return {"data": data}


def build_label_config_xml() -> str:
    """Return the deterministic Label Studio labeling configuration XML.

    Returns:
        XML showing audio, read-only reference text, editable transcript, and
        required review status.
    """
    return "\n".join(
        [
            "<View>",
            '  <Audio name="audio" value="$audio" />',
            (
                '  <Text name="reference_transcript" '
                'value="$reference_transcript" />'
            ),
            (
                '  <TextArea name="transcription" toName="audio" '
                'value="$reference_transcript" editable="true" '
                'transcription="true" rows="4" />'
            ),
            (
                '  <Choices name="review_status" toName="audio" '
                'choice="single" required="true">'
            ),
            '    <Choice value="Reviewed" />',
            '    <Choice value="Skip" />',
            "  </Choices>",
            "</View>",
            "",
        ]
    )


def preview_row_from_ranked_row(
    row: collections.abc.Mapping[str, object],
) -> dict[str, object]:
    """Build one human-readable preview CSV row.

    Args:
        row: Phase 2 ranked row.

    Returns:
        Preview row with reference text and audit fields, excluding model
        prediction text.

    Raises:
        ValueError: If a required ranked-row field is missing.
    """
    _validate_required_fields(row)
    preview = {
        "reference_transcript": row["text"],
    }
    for field in PREVIEW_CSV_FIELDS:
        if field == "reference_transcript":
            continue
        preview[field] = row[field]
    return preview


def render_operator_readme(
    bucket_uri: str = "gs://wd-transcription-data",
) -> str:
    """Return operator instructions for the Label Studio review package.

    Args:
        bucket_uri: Private GCS bucket URI used by Label Studio source storage.

    Returns:
        Markdown README text.
    """
    return "\n".join(
        [
            "# Label Studio Review Package",
            "",
            "## Artifacts",
            "",
            "- `tasks.json`: Label Studio task import JSON.",
            "- `label_config.xml`: Label Studio labeling config.",
            "- `preview.csv`: ranked review queue preview.",
            "",
            "## Private GCS audio access",
            "",
            "Configure Label Studio GCS source storage before import.",
            "Grant the existing Label Studio service account object read access:",
            "",
            "```sh",
            f"gcloud storage buckets add-iam-policy-binding {bucket_uri} \\",
            '  --member="serviceAccount:LABEL_STUDIO_SERVICE_ACCOUNT" \\',
            '  --role="roles/storage.objectViewer"',
            "```",
            "",
            "Do not make audio public and do not replace `gs://` task audio "
            "values with signed URLs for this package.",
            "",
            "## Review outcomes",
            "",
            "- `Reviewed`: the submitted transcript is authoritative.",
            "- `Skip`: no usable correction decision was made.",
            "- `Reviewed` with an empty transcript means exclude this audio "
            "from future eval/train inputs.",
            "",
        ]
    )


def build_package(
    ranked_rows: collections.abc.Iterable[collections.abc.Mapping[str, object]],
    *,
    limit: int = DEFAULT_LIMIT,
    bucket_uri: str = "gs://wd-transcription-data",
) -> LabelStudioPackage:
    """Build all Label Studio review package content.

    Args:
        ranked_rows: Phase 2 ranked rows.
        limit: Maximum number of top-ranked rows to package.
        bucket_uri: Private GCS bucket URI for README instructions.

    Returns:
        Generated task JSON, labeling XML, preview rows, and README text.
    """
    selected_rows = select_ranked_rows(ranked_rows, limit=limit)
    return LabelStudioPackage(
        tasks=[task_from_ranked_row(row) for row in selected_rows],
        label_config_xml=build_label_config_xml(),
        preview_rows=[
            preview_row_from_ranked_row(row) for row in selected_rows
        ],
        readme_text=render_operator_readme(bucket_uri=bucket_uri),
    )


def _validate_required_fields(
    row: collections.abc.Mapping[str, object],
) -> None:
    for field in REQUIRED_RANKED_FIELDS:
        if field not in row:
            raise ValueError(f"ranked row missing required field: {field}")
