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

gemini_sft's strict canonical validator (common.manifest.
strict_canonical_rows_from_manifest, used by gemini-sft prepare) requires
literal example_id/segment_id fields on every raw row -- the "derive from
the audio filename" fallback documented on rows_from_manifest only
applies to a different, more lenient conversion path prepare does not
use. The per-dataset schema build_bcfy_calls_oversampled_manifest.py
reads from never sets those fields, so this script also synthesizes them
here: example_id from the (now-unique, per the GCS-copy fix) audio
filename stem, segment_id fixed at "001", matching that documented
fallback's own semantics.

validation.jsonl's bcfy_calls rows are left out of scope: a leakage check
(source_group overlap between the new oversampled train recordings and
validation.jsonl's existing bcfy_calls rows) found zero overlap, so
nothing needs replacing there. Its split field does need correcting,
though: the production validation.jsonl is intentionally the same
manifest reused for both validation and eval (a documented, supported
gemini_sft pattern), so every row's split field literally reads "eval" --
which the strict validator rejects when the manifest is loaded as
validation (expected_split="validation"). This is a pre-existing gap
between that documented reuse pattern and the current validator, not
something this script's changes caused; it would block any fresh prepare
run reusing this exact validation manifest. Since split is an optional
field, this script copies validation.jsonl into the same output prefix
with split corrected to "validation" on every row, leaving the original
production file untouched.

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
      --source-validation gs://wd-transcription-data/sft/runs/\
20260712-gemini31-flash-lite-a16-lr05-e14-r3/manifests/canonical/\
validation.jsonl \
      --out-train-uri gs://wd-transcription-data/sft/runs/\
2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/train.jsonl \
      --out-eval-uri gs://wd-transcription-data/sft/runs/\
2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/eval.jsonl \
      --out-validation-uri gs://wd-transcription-data/sft/runs/\
2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/validation.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

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


def _tag_bcfy_calls(row: dict, *, split: str) -> dict:
    """Return a copy of row with fields the strict canonical validator needs.

    Args:
        row: A row produced by build_bcfy_calls_oversampled_manifest.py,
            which does not set the nested dataset.family/dataset.name
            fields the production canonical manifest uses, nor the
            example_id/segment_id fields the strict validator requires.
            Its split field, if present, reflects the per-dataset source
            manifest's original split and may not match where this
            script's own recording-level re-split actually placed the
            row (a row's recording can move from the source eval split
            into this script's new train split, or vice versa).
        split: The split this row is being written into ("train" or
            "eval"), which overrides any stale split value on row.

    Returns:
        A copy of row carrying dataset = {"family": "bcfy_calls",
        "name": "bcfy_calls"} (preserving any dataset field it already
        had otherwise), example_id/segment_id derived from the row's own
        audio_filepath -- unique per row, since
        build_bcfy_calls_oversampled_manifest.py gives every duplicate
        its own distinct GCS object -- and split set to the given value.
    """
    tagged = dict(row)
    tagged["dataset"] = {**BCFY_CALLS_DATASET, **(row.get("dataset") or {})}
    tagged.setdefault("example_id", Path(row["audio_filepath"]).stem)
    tagged.setdefault("segment_id", "001")
    tagged["split"] = split
    return tagged


def _copy_validation_split_fixed(
    client: storage.Client, *, source_uri: str, out_uri: str
) -> None:
    """Copy validation.jsonl with a corrected split field on every row.

    Args:
        client: Storage client used for gs:// reads/writes.
        source_uri: Production canonical validation.jsonl (gs:// or
            local), whose rows carry split="eval" since it is
            intentionally the same manifest reused for both eval and
            validation.
        out_uri: Destination for the corrected copy.
    """
    rows = read_jsonl(client, source_uri)
    fixed_rows = [{**row, "split": "validation"} for row in rows]
    write_jsonl(client, fixed_rows, out_uri)
    logger.info(
        "validation: copied %s rows from %s with split corrected -> %s",
        len(fixed_rows),
        source_uri,
        out_uri,
    )


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
        _tag_bcfy_calls(row, split=split_label)
        for row in read_jsonl(client, new_rows_path)
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
        "--source-validation",
        default=(
            "gs://wd-transcription-data/sft/runs/"
            "20260712-gemini31-flash-lite-a16-lr05-e14-r3/manifests/"
            "canonical/validation.jsonl"
        ),
        help=(
            "Production canonical validation.jsonl (gs:// or local), "
            "copied with its split field corrected -- see module "
            "docstring."
        ),
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
    parser.add_argument(
        "--out-validation-uri",
        default=(
            "gs://wd-transcription-data/sft/runs/"
            "2026-07-26-bcfy-calls-8khz-oversample/manifests/canonical/"
            "validation.jsonl"
        ),
        help="Destination for the split-corrected validation.jsonl copy.",
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
    _copy_validation_split_fixed(
        client,
        source_uri=args.source_validation,
        out_uri=args.out_validation_uri,
    )


if __name__ == "__main__":
    main()
