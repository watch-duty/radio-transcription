"""Merge the bcfy_calls oversampled/holdout manifests (built by
build_bcfy_calls_oversampled_manifest.py) into the real production
canonical train/eval manifests for round 20260712-gemini31-flash-lite-a16-
lr05-e14-r3, replacing that round's original bcfy_calls rows.

The production canonical train.jsonl carries a nested
dataset = {"family": ..., "name": ...} object (not the flat
dataset_family/dataset_name fields used by the per-dataset manifests this
repo's other bcfy_calls tooling reads). This script removes every row
with dataset.family == "bcfy_calls" from the source train/eval manifests
and appends the new oversampled/holdout rows in their place, tagging each
appended row with dataset = {"family": "bcfy_calls", "name": "bcfy_calls"}
for consistency with the rest of the file.

validation.jsonl is intentionally left out of scope: a leakage check
(source_group overlap between the new oversampled train recordings and
validation.jsonl's existing bcfy_calls rows) found zero overlap, so the
original validation.jsonl can be referenced unmodified in the eventual
run_config.toml.

Output is written under a distinct round prefix rather than overwriting
anything in the production round's own manifests/canonical/ path -- see
--out-train-uri/--out-eval-uri, which default to the
2026-07-26-bcfy-calls-8khz-oversample round used for this experiment.

Usage:
  uv run --with google-cloud-storage \
      python merge_bcfy_calls_oversampled_into_canonical.py \
      --source-train gs://wd-transcription-data/sft/runs/\
20260712-gemini31-flash-lite-a16-lr05-e14-r3/manifests/canonical/train.jsonl \
      --source-eval gs://wd-transcription-data/sft/runs/\
20260712-gemini31-flash-lite-a16-lr05-e14-r3/manifests/canonical/eval.jsonl \
      --new-train bcfy_calls_oversampled_train.jsonl \
      --new-eval bcfy_calls_holdout_eval.jsonl \
      --project <gcp-project> \
      --out-train-uri gs://wd-transcription-data/sft/runs/\
2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/train.jsonl \
      --out-eval-uri gs://wd-transcription-data/sft/runs/\
2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/eval.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging

from google.cloud import storage

logger = logging.getLogger(__name__)

BCFY_CALLS_DATASET = {"family": "bcfy_calls", "name": "bcfy_calls"}


def read_jsonl(client: storage.Client, path: str) -> list[dict]:
    """Read a JSONL manifest from a local path or a gs:// URI.

    Args:
        client: Storage client used for gs:// reads.
        path: Local filesystem path or gs:// URI to a JSONL file.

    Returns:
        The parsed rows in file order.
    """
    if path.startswith("gs://"):
        bucket_name, blob_name = path[len("gs://") :].split("/", 1)
        text = client.bucket(bucket_name).blob(blob_name).download_as_text()
    else:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def write_jsonl(client: storage.Client, rows: list[dict], path: str) -> None:
    """Write rows as JSONL to a local path or a gs:// URI.

    Args:
        client: Storage client used for gs:// writes.
        rows: Rows to serialize in order.
        path: Local filesystem path or gs:// destination URI.
    """
    text = "\n".join(json.dumps(row) for row in rows) + "\n"
    if path.startswith("gs://"):
        bucket_name, blob_name = path[len("gs://") :].split("/", 1)
        client.bucket(bucket_name).blob(blob_name).upload_from_string(text)
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)


def _is_bcfy_calls(row: dict) -> bool:
    dataset = row.get("dataset")
    return isinstance(dataset, dict) and dataset.get("family") == "bcfy_calls"


def _tag_bcfy_calls(row: dict) -> dict:
    """Return a copy of row with an explicit bcfy_calls dataset tag.

    Args:
        row: A row produced by build_bcfy_calls_oversampled_manifest.py,
            which does not set the nested dataset.family/dataset.name
            fields the production canonical manifest uses.

    Returns:
        A copy of row carrying dataset = {"family": "bcfy_calls",
        "name": "bcfy_calls"}, preserving any dataset field it already
        had otherwise.
    """
    tagged = dict(row)
    tagged["dataset"] = {**BCFY_CALLS_DATASET, **(row.get("dataset") or {})}
    return tagged


def _merge_split(
    client: storage.Client,
    *,
    source_uri: str,
    new_rows_path: str,
    out_uri: str,
    split_label: str,
) -> None:
    source_rows = read_jsonl(client, source_uri)
    n_removed = sum(1 for row in source_rows if _is_bcfy_calls(row))
    kept_rows = [row for row in source_rows if not _is_bcfy_calls(row)]
    new_rows = [
        _tag_bcfy_calls(row) for row in read_jsonl(client, new_rows_path)
    ]

    merged_rows = kept_rows + new_rows
    logger.info(
        "%s: %s source rows, removed %s old bcfy_calls rows, added %s new "
        "rows -> %s total",
        split_label,
        len(source_rows),
        n_removed,
        len(new_rows),
        len(merged_rows),
    )
    write_jsonl(client, merged_rows, out_uri)
    logger.info("%s: wrote %s", split_label, out_uri)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-train",
        required=True,
        help="Production canonical train.jsonl (gs:// or local).",
    )
    parser.add_argument(
        "--source-eval",
        required=True,
        help="Production canonical eval.jsonl (gs:// or local).",
    )
    parser.add_argument(
        "--new-train",
        required=True,
        help="Output of build_bcfy_calls_oversampled_manifest.py --out-train.",
    )
    parser.add_argument(
        "--new-eval",
        required=True,
        help="Output of build_bcfy_calls_oversampled_manifest.py --out-eval.",
    )
    parser.add_argument(
        "--project", default=None, help="GCP project for GCS access"
    )
    parser.add_argument(
        "--out-train-uri",
        default=(
            "gs://wd-transcription-data/sft/runs/"
            "2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/"
            "train.jsonl"
        ),
        help="Destination for the merged canonical train.jsonl.",
    )
    parser.add_argument(
        "--out-eval-uri",
        default=(
            "gs://wd-transcription-data/sft/runs/"
            "2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/"
            "eval.jsonl"
        ),
        help="Destination for the merged canonical eval.jsonl.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    client = storage.Client(project=args.project)

    _merge_split(
        client,
        source_uri=args.source_train,
        new_rows_path=args.new_train,
        out_uri=args.out_train_uri,
        split_label="train",
    )
    _merge_split(
        client,
        source_uri=args.source_eval,
        new_rows_path=args.new_eval,
        out_uri=args.out_eval_uri,
        split_label="eval",
    )


if __name__ == "__main__":
    main()
