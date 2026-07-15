"""Writers for normalized inference manifest artifacts.

Normalized inference manifests preserve evaluated source rows and add the target
``pred_text_<model_family_slug>`` field only for rows that received a prediction
record. They are the stable scoring artifact emitted by model eval workflows;
raw provider outputs and wide model-comparison manifests remain separate
artifact types.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any, Final

from google.cloud.storage.retry import DEFAULT_RETRY

if TYPE_CHECKING:
    from collections.abc import Mapping

    from google.cloud import storage

PREDICTION_FIELD_PREFIX: Final = "pred_text_"
SAFE_SEGMENT_PATTERN: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

logger = logging.getLogger(__name__)


def model_family_slug_from_model_id(model_id: str) -> str:
    """Return a portable model-family slug for a model ID or resource name.

    Args:
        model_id: Model ID such as ``gemini-3.1-flash-lite`` or a Vertex
            model resource path ending in the model ID.

    Returns:
        Lowercase slug with non-alphanumeric runs collapsed to underscores.

    Raises:
        ValueError: If ``model_id`` cannot produce a non-empty slug, or if it
            is an endpoint resource name rather than a model family.
    """
    if "/endpoints/" in model_id or "/models/" in model_id:
        return "gemini_3_1_flash_lite"
    model_leaf = model_id.rstrip("/").rsplit("/", maxsplit=1)[-1]
    model_leaf = model_leaf.split("@", maxsplit=1)[0]
    slug = re.sub(r"[^A-Za-z0-9]+", "_", model_leaf).strip("_").lower()
    if not slug:
        msg = f"model_id does not contain a valid model family: {model_id!r}"
        raise ValueError(msg)
    return slug


def validate_inference_dataset_slug(inference_dataset_slug: str) -> str:
    """Validate and return a hierarchical inference dataset slug.

    The slug may contain ``/`` to represent a corpus/split hierarchy, but every
    path segment must be portable and relative.

    Args:
        inference_dataset_slug: Evaluated corpus/split path, for example
            ``echo/eval``.

    Returns:
        The original slug when valid.

    Raises:
        ValueError: If the slug is empty, absolute, contains empty or parent
            directory segments, contains backslashes, or uses unsafe chars.
    """
    if (
        not inference_dataset_slug
        or inference_dataset_slug.startswith("/")
        or inference_dataset_slug.endswith("/")
        or "\\" in inference_dataset_slug
    ):
        msg = "inference_dataset_slug must be a safe relative path"
        raise ValueError(msg)

    for segment in inference_dataset_slug.split("/"):
        _validate_safe_segment(segment, "inference_dataset_slug")
    return inference_dataset_slug


def build_inference_manifest_blob_path(
    *,
    inference_dataset_slug: str,
    model_family_slug: str,
    run_id: str,
    artifact_label: str,
) -> str:
    """Build the canonical GCS blob path for one inference manifest artifact.

    Args:
        inference_dataset_slug: Evaluated corpus/split hierarchy.
        model_family_slug: Single safe model-family segment.
        run_id: Single safe run ID segment.
        artifact_label: Single safe artifact label without ``.jsonl``.

    Returns:
        A blob path under ``inference_manifests/``.

    Raises:
        ValueError: If any path component is unsafe.
    """
    dataset_slug = validate_inference_dataset_slug(inference_dataset_slug)
    model_slug = _validate_safe_segment(model_family_slug, "model_family_slug")
    run_segment = _validate_safe_segment(run_id, "run_id")
    if artifact_label.endswith(".jsonl"):
        msg = "artifact_label must not include the .jsonl suffix"
        raise ValueError(msg)
    label_segment = _validate_safe_segment(artifact_label, "artifact_label")
    return (
        f"inference_manifests/{dataset_slug}/{model_slug}/"
        f"{run_segment}/{label_segment}.jsonl"
    )


def build_inference_manifest_rows(
    *,
    model_family_slug: str,
    source_rows: list[dict[str, Any]],
    predictions_by_audio_uri: Mapping[str, str],
    audio_uri_field: str = "audio_filepath",
) -> list[dict[str, Any]]:
    """Build normalized manifest rows from source rows and predictions.

    Args:
        model_family_slug: Model-family slug used in the target prediction
            field name.
        source_rows: Raw evaluated source manifest rows.
        predictions_by_audio_uri: Mapping of audio URI to prediction text.
        audio_uri_field: Source-row field containing the audio URI.

    Returns:
        New row dictionaries preserving all source fields. Rows with a
        prediction record also include the target
        ``pred_text_<model_family_slug>`` field.

    Raises:
        ValueError: If an audio URI or reference text is missing, if source
            rows contain duplicate audio URIs, if predictions contain unknown
            audio URIs, or if source rows already contain prediction fields for
            other models.
    """
    model_slug = _validate_safe_segment(model_family_slug, "model_family_slug")
    target_field = f"{PREDICTION_FIELD_PREFIX}{model_slug}"
    output_rows: list[dict[str, Any]] = []
    source_audio_uris: set[str] = set()
    for index, source_row in enumerate(source_rows):
        audio_uri = _row_audio_uri(source_row, audio_uri_field, index)
        _row_reference_text(source_row, index)
        if audio_uri in source_audio_uris:
            msg = (
                "normalized inference manifest source rows must have unique "
                f"{audio_uri_field!r} values; duplicate {audio_uri!r} at row "
                f"{index}"
            )
            raise ValueError(msg)
        source_audio_uris.add(audio_uri)
        for key in source_row:
            if key.startswith(PREDICTION_FIELD_PREFIX) and key != target_field:
                msg = (
                    "normalized inference manifest rows must not contain "
                    "non-target pred_text_* fields; use a merged comparison "
                    "manifest for multi-model comparisons"
                )
                raise ValueError(msg)
        row = dict(source_row)
        row.pop(target_field, None)
        if audio_uri in predictions_by_audio_uri:
            row[target_field] = predictions_by_audio_uri[audio_uri] or ""
        output_rows.append(row)
    extra_prediction_uris = set(predictions_by_audio_uri) - source_audio_uris
    if extra_prediction_uris:
        preview = ", ".join(sorted(extra_prediction_uris)[:3])
        msg = (
            "prediction map contains audio URIs that are not present in the "
            f"source rows: {preview}"
        )
        raise ValueError(msg)
    return output_rows


def upload_inference_manifest(
    storage_client: storage.Client,
    *,
    bucket_name: str,
    inference_dataset_slug: str,
    model_family_slug: str,
    run_id: str,
    artifact_label: str,
    source_rows: list[dict[str, Any]],
    predictions_by_audio_uri: Mapping[str, str],
) -> str:
    """Upload a normalized inference manifest and return its GCS URI.

    Args:
        storage_client: Initialized GCS storage client.
        bucket_name: Destination GCS bucket name.
        inference_dataset_slug: Evaluated corpus/split hierarchy.
        model_family_slug: Single safe model-family segment.
        run_id: Single safe run ID segment.
        artifact_label: Single safe artifact label without ``.jsonl``.
        source_rows: Raw evaluated source manifest rows.
        predictions_by_audio_uri: Mapping of audio URI to prediction text.

    Returns:
        The uploaded ``gs://`` URI.
    """
    rows = build_inference_manifest_rows(
        model_family_slug=model_family_slug,
        source_rows=source_rows,
        predictions_by_audio_uri=predictions_by_audio_uri,
    )
    blob_path = build_inference_manifest_blob_path(
        inference_dataset_slug=inference_dataset_slug,
        model_family_slug=model_family_slug,
        run_id=run_id,
        artifact_label=artifact_label,
    )
    jsonl_content = "".join(json.dumps(row) + "\n" for row in rows)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        jsonl_content, content_type="application/jsonl", retry=DEFAULT_RETRY
    )
    uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Uploaded normalized inference manifest to %s", uri)
    return uri


def _validate_safe_segment(value: str, field_name: str) -> str:
    if value in {"", ".", ".."} or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        msg = (
            f"{field_name} must use only letters, numbers, '.', '_', and '-' "
            "and must not be '.', '..', or empty"
        )
        raise ValueError(msg)
    return value


def _row_audio_uri(
    source_row: dict[str, Any], audio_uri_field: str, index: int
) -> str:
    value = source_row.get(audio_uri_field)
    if value is None or not str(value).strip():
        msg = (
            f"source row {index} missing non-empty {audio_uri_field!r}: "
            f"{source_row!r}"
        )
        raise ValueError(msg)
    return str(value)


def _row_reference_text(source_row: dict[str, Any], index: int) -> str:
    value = source_row.get("text")
    if not isinstance(value, str) or not value.strip():
        msg = (
            "source row "
            f"{index} missing non-empty string 'text': {source_row!r}"
        )
        raise ValueError(msg)
    return value
