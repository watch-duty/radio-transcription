#!/usr/bin/env python3
"""Build canonical manifests for the count-8 prior-context Gemini SFT run."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from google.cloud import storage

from common.gcs_utils import parse_gcs_uri

DEFAULT_OUTPUT_PREFIX = (
    "gs://wd-transcription-data/sft/experiments/"
    "gemini-prior-context-sft-v20260625/manifests"
)

DEFAULT_TRAIN_MANIFESTS = [
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_calls/train.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_feeds/train.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/echo/train.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "fire_notifications/train.jsonl",
]

DEFAULT_EVAL_MANIFESTS = [
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_calls/eval.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "bcfy_feeds/eval.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/echo/eval.jsonl",
    "gs://wd-transcription-data/sft/dataset_versions/"
    "radio-transcription-sft-v20260528/manifests/per_dataset/"
    "fire_notifications/eval.jsonl",
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--project", default=None)
    parser.add_argument(
        "--local-output-dir",
        type=Path,
        default=None,
        help="Optional directory for local manifest copies.",
    )
    args = parser.parse_args()

    storage_client = storage.Client(project=args.project)
    train_rows = _load_many(storage_client, DEFAULT_TRAIN_MANIFESTS)
    eval_rows = _load_many(storage_client, DEFAULT_EVAL_MANIFESTS)

    canonical_train = _canonicalize(train_rows, split="train")
    canonical_eval = _canonicalize(eval_rows, split="eval")
    canonical_validation = [
        {**row, "split": "validation"} for row in canonical_eval
    ]

    output_prefix = args.output_prefix.rstrip("/")
    outputs = {
        f"{output_prefix}/train.jsonl": canonical_train,
        f"{output_prefix}/validation.jsonl": canonical_validation,
        f"{output_prefix}/eval.jsonl": canonical_eval,
    }
    for uri, rows in outputs.items():
        text = "".join(json.dumps(row) + "\n" for row in rows)
        _upload_text(storage_client, uri, text)
        if args.local_output_dir is not None:
            args.local_output_dir.mkdir(parents=True, exist_ok=True)
            (args.local_output_dir / Path(uri).name).write_text(
                text,
                encoding="utf-8",
            )
        print(f"wrote {len(rows)} rows to {uri}")

    return 0


def _load_many(
    storage_client: storage.Client, manifest_uris: list[str]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for uri in manifest_uris:
        loaded = _load_jsonl(storage_client, uri)
        for row in loaded:
            row.setdefault("_source_manifest_uri", uri)
        rows.extend(loaded)
        print(f"loaded {len(loaded)} rows from {uri}")
    return rows


def _load_jsonl(
    storage_client: storage.Client, uri: str
) -> list[dict[str, Any]]:
    bucket_name, blob_path = parse_gcs_uri(uri)
    text = storage_client.bucket(bucket_name).blob(blob_path).download_as_text()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _canonicalize(
    rows: list[dict[str, Any]], *, split: str
) -> list[dict[str, Any]]:
    source_positions = _source_positions(rows)
    output_rows: list[dict[str, Any]] = []
    for input_index, row in enumerate(rows):
        canonical = dict(row)
        audio_uri = _required_str(canonical, "audio_filepath", input_index)
        text = _required_str(canonical, "text", input_index)
        source_key = _episode_key(canonical)
        dataset_slug = _dataset_slug(canonical)
        canonical["audio_filepath"] = audio_uri
        canonical["text"] = text
        canonical["example_id"] = _example_id(dataset_slug, source_key)
        canonical["segment_id"] = (
            f"seg-{source_positions[source_key][input_index]:06d}"
        )
        canonical["offset"] = _first_numeric(
            canonical.get("offset"),
            canonical.get("original_offset"),
            default=0.0,
        )
        canonical["duration"] = float(canonical.get("duration") or 0.0)
        canonical["split"] = split
        output_rows.append(canonical)
    return output_rows


def _source_positions(rows: list[dict[str, Any]]) -> dict[str, dict[int, int]]:
    grouped: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for input_index, row in enumerate(rows):
        grouped[_episode_key(row)].append((input_index, row))

    positions: dict[str, dict[int, int]] = {}
    for source_key, indexed_rows in grouped.items():
        sorted_rows = sorted(
            indexed_rows,
            key=lambda item: (
                _first_numeric(
                    item[1].get("original_offset"),
                    item[1].get("offset"),
                    default=float(item[0]),
                ),
                _first_numeric(
                    item[1].get("row_index"), default=float(item[0])
                ),
                item[1].get("audio_filepath") or "",
            ),
        )
        positions[source_key] = {
            input_index: position
            for position, (input_index, _) in enumerate(sorted_rows)
        }
    return positions


def _episode_key(row: dict[str, Any]) -> str:
    for key in ("original_audio_uri", "audio_uri", "source_group"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return _required_str(row, "audio_filepath", -1)


def _dataset_slug(row: dict[str, Any]) -> str:
    for key in ("dataset_name", "dataset_family", "source_group"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return _safe_segment(value)
    source_uri = str(row.get("_source_manifest_uri") or "dataset")
    return _safe_segment(source_uri.rstrip("/").split("/")[-2])


def _example_id(dataset_slug: str, source_key: str) -> str:
    digest = hashlib.sha1(source_key.encode("utf-8")).hexdigest()[:16]
    return f"{dataset_slug}-{digest}"


def _safe_segment(value: str) -> str:
    safe = "".join(ch if ch.isalnum() else "-" for ch in value.lower())
    safe = "-".join(part for part in safe.split("-") if part)
    return safe or "dataset"


def _required_str(row: dict[str, Any], key: str, index: int) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        msg = f"row {index} missing non-empty {key}"
        raise ValueError(msg)
    return value.strip()


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _first_numeric(*values: Any, default: float) -> float:
    for value in values:
        parsed = _numeric_value(value)
        if parsed is not None:
            return parsed
    return default


def _upload_text(
    storage_client: storage.Client,
    uri: str,
    text: str,
) -> None:
    bucket_name, blob_path = parse_gcs_uri(uri)
    storage_client.bucket(bucket_name).blob(blob_path).upload_from_string(
        text,
        content_type="application/jsonl",
    )


if __name__ == "__main__":
    raise SystemExit(main())
