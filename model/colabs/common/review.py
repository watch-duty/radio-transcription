"""Review-row identity helpers for transcript correction workflows."""

from __future__ import annotations

import collections.abc
import dataclasses
import decimal
import hashlib
import json

import google.cloud.storage.retry

from common import gcs_utils


REQUIRED_REVIEW_ROW_FIELDS = (
    "model_ready_audio_uri",
    "original_audio_uri",
    "offset",
    "duration",
    "source_group",
    "row_index",
    "split",
    "dataset_name",
    "text",
)


@dataclasses.dataclass(frozen=True)
class GcsObjectMetadata:
    """Audit metadata for the exact model-ready GCS audio object.

    Attributes:
        uri: Model-ready `gs://` URI.
        md5_hash: Server-reported MD5 hash for the GCS object.
        crc32c_hash: Server-reported CRC32C checksum, if available.
        size: Object size in bytes.
        generation: GCS object generation, if available.
        storage_url: Storage URL retained for audit display.
    """

    uri: str
    md5_hash: str
    crc32c_hash: str | None
    size: int
    generation: str | None
    storage_url: str = ""


@dataclasses.dataclass(frozen=True)
class ReviewManifestRow:
    """Validated canonical row for reference transcript review.

    Attributes:
        model_ready_audio_uri: Exact audio URI humans and models consume.
        original_audio_uri: Source recording URI used for provenance.
        offset: Source offset value from the canonical manifest.
        duration: Source duration value from the canonical manifest.
        source_group: Source grouping key used by ranking context.
        row_index: Row index from the canonical manifest.
        split: Original manifest split label.
        dataset_name: Dataset name from the canonical manifest.
        text: Current reference transcript.
    """

    model_ready_audio_uri: str
    original_audio_uri: str
    offset: object
    duration: object
    source_group: str
    row_index: int
    split: str
    dataset_name: str
    text: str

    def to_dict(self) -> dict[str, object]:
        """Return the row as a plain dict with canonical field names."""
        return {
            "model_ready_audio_uri": self.model_ready_audio_uri,
            "original_audio_uri": self.original_audio_uri,
            "offset": self.offset,
            "duration": self.duration,
            "source_group": self.source_group,
            "row_index": self.row_index,
            "split": self.split,
            "dataset_name": self.dataset_name,
            "text": self.text,
        }


ReviewRowInput = ReviewManifestRow | collections.abc.Mapping[str, object]


def load_review_manifest_rows(
    manifest_rows_by_name: collections.abc.Mapping[
        str,
        collections.abc.Iterable[collections.abc.Mapping[str, object]],
    ],
) -> list[ReviewManifestRow]:
    """Validate and combine canonical manifests into one review pool.

    Args:
        manifest_rows_by_name: Mapping from manifest label, such as `train`
            or `eval`, to raw canonical row dicts.

    Returns:
        Validated review rows in manifest label order and row order.

    Raises:
        ValueError: If a row is missing a required review field.
    """
    rows: list[ReviewManifestRow] = []
    for manifest_name, manifest_rows in manifest_rows_by_name.items():
        for row_number, raw_row in enumerate(manifest_rows):
            rows.append(
                _review_row_from_mapping(
                    raw_row,
                    manifest_name=manifest_name,
                    row_number=row_number,
                )
            )
    return rows


def download_review_manifest_rows(
    storage_client: object,
    manifest_uris: collections.abc.Mapping[str, str],
) -> list[ReviewManifestRow]:
    """Download canonical JSONL manifests from GCS and validate review rows.

    Args:
        storage_client: Initialized Google Cloud Storage client.
        manifest_uris: Mapping from manifest label to `gs://` JSONL URI.

    Returns:
        Combined validated review rows.
    """
    manifest_rows_by_name = {}
    for manifest_name, manifest_uri in manifest_uris.items():
        manifest_rows_by_name[manifest_name] = (
            gcs_utils.download_jsonl_manifest(storage_client, manifest_uri)
        )
    return load_review_manifest_rows(manifest_rows_by_name)


def fetch_gcs_object_metadata(
    storage_client: object,
    gcs_uri: str,
    *,
    timeout: float | tuple[float, float] | None = None,
) -> GcsObjectMetadata:
    """Fetch exact-content metadata for a GCS object.

    Args:
        storage_client: Initialized Google Cloud Storage client.
        gcs_uri: A `gs://` URI to one model-ready audio object.
        timeout: Optional request timeout passed to `Blob.reload`.

    Returns:
        GCS object metadata used for exact-audio identity and audit fields.

    Raises:
        ValueError: If GCS does not expose `md5_hash` or `size`.
    """
    bucket_name, blob_path = gcs_utils.parse_gcs_uri(gcs_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    if timeout is None:
        blob.reload(retry=google.cloud.storage.retry.DEFAULT_RETRY)
    else:
        blob.reload(
            retry=google.cloud.storage.retry.DEFAULT_RETRY,
            timeout=timeout,
        )

    md5_hash = blob.md5_hash
    if not md5_hash:
        raise ValueError(f"GCS object missing md5_hash: {gcs_uri}")
    size = blob.size
    if size is None:
        raise ValueError(f"GCS object missing size: {gcs_uri}")

    generation = None if blob.generation is None else str(blob.generation)
    return GcsObjectMetadata(
        uri=gcs_uri,
        md5_hash=str(md5_hash),
        crc32c_hash=blob.crc32c,
        size=int(size),
        generation=generation,
        storage_url=gcs_uri,
    )


def audio_segment_id(metadata: GcsObjectMetadata) -> str:
    """Return the exact-audio identity from GCS content metadata.

    Args:
        metadata: Model-ready GCS object metadata.

    Returns:
        Stable `audio-` prefixed SHA-256 identity.

    Raises:
        ValueError: If MD5 hash or size is missing.
    """
    if not metadata.md5_hash:
        raise ValueError(f"GCS object missing md5_hash: {metadata.uri}")
    if metadata.size is None:
        raise ValueError(f"GCS object missing size: {metadata.uri}")
    return _stable_hash(
        "audio",
        {
            "md5_hash": metadata.md5_hash,
            "size": metadata.size,
        },
    )


def source_window_id(row: ReviewRowInput) -> str:
    """Return source-window provenance identity for a review row.

    Args:
        row: Validated review row or raw row mapping.

    Returns:
        Stable `source-` prefixed SHA-256 identity.
    """
    return _stable_hash(
        "source",
        {
            "original_audio_uri": _row_value(row, "original_audio_uri"),
            "offset": _canonical_decimal(_row_value(row, "offset")),
            "duration": _canonical_decimal(_row_value(row, "duration")),
            "source_group": _row_value(row, "source_group"),
        },
    )


def _review_row_from_mapping(
    raw_row: collections.abc.Mapping[str, object],
    *,
    manifest_name: str,
    row_number: int,
) -> ReviewManifestRow:
    missing = [
        field for field in REQUIRED_REVIEW_ROW_FIELDS if field not in raw_row
    ]
    if missing:
        raise ValueError(
            "review manifest row missing required field "
            f"{missing[0]!r} in {manifest_name} row {row_number}"
        )

    return ReviewManifestRow(
        model_ready_audio_uri=str(raw_row["model_ready_audio_uri"]),
        original_audio_uri=str(raw_row["original_audio_uri"]),
        offset=raw_row["offset"],
        duration=raw_row["duration"],
        source_group=str(raw_row["source_group"]),
        row_index=int(raw_row["row_index"]),
        split=str(raw_row["split"]),
        dataset_name=str(raw_row["dataset_name"]),
        text="" if raw_row["text"] is None else str(raw_row["text"]),
    )


def _row_value(row: ReviewRowInput, field: str) -> object:
    if dataclasses.is_dataclass(row):
        return getattr(row, field)
    return row[field]


def _stable_hash(prefix: str, payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"{prefix}-{digest}"


def _canonical_decimal(value: object) -> str:
    number = decimal.Decimal(str(value))
    if number == 0:
        number = decimal.Decimal(0)
    normalized = number.normalize()
    return format(normalized, "f")
