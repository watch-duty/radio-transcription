"""Build a canonical validation manifest by sampling an eval manifest.

Vertex AI SFT requires the validation set to be smaller than 5000 rows, and
this team's convention is to build validation.jsonl as a sample of
eval.jsonl with every row's split relabeled to "validation" (eval and
validation are intentionally the same underlying data, viewed through two
different splits). This script exists so that convention is captured in
code instead of living only in one operator's head -- see
docs/runbook.md's "Build A Validation Manifest" section for why this
script exists and when to use it.

Usage:

    uv run python build_validation_manifest_from_eval.py --eval
    gs://bucket/path/manifests/canonical/eval.jsonl --out-validation-uri
    gs://bucket/path/manifests/canonical/validation.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import random

from google.cloud import storage
from google.cloud.storage import retry as storage_retry

logger = logging.getLogger(__name__)
_VERTEX_VALIDATION_ROW_CAP = 5000


def read_jsonl(
    path: str, *, client: storage.Client | None = None
) -> list[dict[str, object]]:
    """Read a JSONL manifest from a gs:// URI or local path.

    Args:
        path: gs:// URI or local filesystem path.
        client: Storage client used for gs:// reads.

    Returns:
        Parsed rows in file order.
    """
    if path.startswith("gs://"):
        if client is None:
            client = storage.Client()
        bucket_name, _, blob_name = path.removeprefix("gs://").partition("/")
        blob = client.bucket(bucket_name).blob(blob_name)
        text = blob.download_as_text(retry=storage_retry.DEFAULT_RETRY)
    else:
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def write_jsonl(
    rows: list[dict[str, object]],
    path: str,
    *,
    client: storage.Client | None = None,
) -> None:
    """Write rows as JSONL to a gs:// URI or local path.

    Args:
        rows: Rows to serialize, one per line.
        path: gs:// URI or local filesystem path.
        client: Storage client used for gs:// writes.
    """
    text = "\n".join(json.dumps(row) for row in rows) + "\n"
    if path.startswith("gs://"):
        if client is None:
            client = storage.Client()
        bucket_name, _, blob_name = path.removeprefix("gs://").partition("/")
        client.bucket(bucket_name).blob(blob_name).upload_from_string(
            text, retry=storage_retry.DEFAULT_RETRY
        )
    else:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)


def build_validation_rows(
    eval_rows: list[dict[str, object]], *, max_rows: int, seed: int
) -> list[dict[str, object]]:
    """Sample eval rows and relabel them as the validation split.

    Args:
        eval_rows: Canonical eval manifest rows.
        max_rows: Maximum rows to keep (Vertex AI's validation-set cap).
        seed: Random seed, so a rerun with the same inputs is reproducible.

    Returns:
        A new list of rows, each with split="validation", in ascending
        original-index order (not shuffled), so it stays a legible subset
        of the eval manifest.

    Raises:
        ValueError: If eval_rows is empty or max_rows is non-positive.
    """
    if not eval_rows:
        msg = "eval_rows must be non-empty"
        raise ValueError(msg)
    if max_rows <= 0:
        msg = f"max_rows must be positive, got {max_rows}"
        raise ValueError(msg)
    if len(eval_rows) <= max_rows:
        sampled_indices = range(len(eval_rows))
    else:
        rng = random.Random(seed)  # noqa: S311 - deterministic sampling, not security.
        sampled_indices = sorted(rng.sample(range(len(eval_rows)), max_rows))
    return [{**eval_rows[i], "split": "validation"} for i in sampled_indices]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--eval",
        required=True,
        help="Source canonical eval.jsonl (gs:// URI or local path).",
    )
    parser.add_argument(
        "--out-validation-uri",
        required=True,
        help="Destination for the sampled, relabeled validation.jsonl.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=_VERTEX_VALIDATION_ROW_CAP,
        help="Row cap for the validation split (default: Vertex AI's "
        f"{_VERTEX_VALIDATION_ROW_CAP}-row limit).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Random seed for sampling when eval has more than --max-rows.",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project for the storage client, if not ambient default.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    client = None
    if args.eval.startswith("gs://") or args.out_validation_uri.startswith(
        "gs://"
    ):
        client = storage.Client(project=args.project)
    eval_rows = read_jsonl(args.eval, client=client)
    validation_rows = build_validation_rows(
        eval_rows, max_rows=args.max_rows, seed=args.seed
    )
    write_jsonl(validation_rows, args.out_validation_uri, client=client)
    logger.info(
        "validation: sampled %s/%s eval rows with split=validation -> %s",
        len(validation_rows),
        len(eval_rows),
        args.out_validation_uri,
    )


if __name__ == "__main__":
    main()
