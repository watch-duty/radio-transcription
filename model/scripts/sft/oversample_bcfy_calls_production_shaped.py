"""Oversample bcfy_calls training rows in Shuojing's production-shaped SFT
dataset reconstruction (PR #1127, gs://wd-transcription-data/sft/
dataset_versions/20260724-production-shaped-reconstruction/).

This supersedes the earlier bcfy_calls oversampling effort against the
prior dataset (radio-transcription-sft-v20260528 / round
20260712-gemini31-flash-lite-a16-lr05-e14-r3 -- see
build_bcfy_calls_oversampled_manifest.py and
merge_bcfy_calls_oversampled_into_canonical.py in this same directory),
which is now expected to be superseded by this reconstruction.

Two things are simpler here than in the prior effort:

1. No Track 1 (recovering eval rows into training, recording-level
   re-split). This dataset's train/eval/validation split is a
   deliberately constructed, independently reviewed, hash-pinned
   "immutable" artifact, and PR #1127 explicitly instructs that any
   future bcfy_calls weighting decision must operate only on
   training-owned rows and must not draw from eval. Oversampling the
   existing 1,179 training-owned bcfy_calls rows alone already lifts
   bcfy_calls to roughly a quarter of the resulting training set --
   comfortably past what the prior effort's combined Track 1+2 was
   targeting -- so there is no practical need to touch eval, and doing
   so would silently move a benchmark another engineer designed to stay
   fixed. This script never reads or writes eval.jsonl/validation.jsonl.

2. No per-row sample-rate measurement. The prior dataset's manifest
   didn't carry real sample rate anywhere, so build_bcfy_calls_
   oversampled_manifest.py downloaded every native row's audio and
   measured it with soundfile. This dataset's rows carry
   source_audio.sample_rate natively, so it's a free field read instead.

Where the 12x/6x defaults come from: a July 2026 incident-window study of
1,934 SFT-failed production segments (plus a matched succeeded sample)
found bcfy_calls segments fail far more often at 8kHz than at 16kHz --
87.5% (525/600) vs. 44.9% (133/296), roughly 2:1. That 2:1 ratio between
--dup-8khz and --dup-other is not a coincidence; it directly encodes
"duplicate the subset that demonstrably fails more, proportionally
harder." The absolute scale (12, not e.g. 8 or 20) was picked to land
bcfy_calls' resulting training share in the ~12-17% range that an earlier
flat-multiplier draft of this same proposal had already been considering
reasonable for this row count, before the sample-rate split was found.
Applied to this dataset's different bcfy_calls composition (478
narrowband / 701 other, vs. the study's 600/296), the same 12x/6x lands
at a higher ~23% share -- not retargeted for this dataset specifically.

These are a single agent's proposed defaults, adopted because the team
was not expected to have a strong opinion on the exact multipliers and
the retune needed to proceed either way -- not an independently reviewed
target. Treat them as a documented starting point to argue with, not a
derived optimum. In particular, the 2:1 severity ratio was measured
against the prior dataset's 830 bcfy_calls rows; nobody has re-verified
it holds for this reconstruction's specific 1,179 rows, which may draw on
different underlying recordings under PR #1127's different segmentation
rules.

Duplicate rows still need a real, distinct GCS audio object each -- the
canonical manifest validator (common.manifest.validate_canonical_
manifest) rejects any two rows in the same manifest sharing an
audio_filepath -- so this script still performs a server-side GCS
copy_blob per duplicate under --dest-prefix, same as the prior effort.
Each duplicate also gets a unique example_id/segment_id (this dataset's
rows already carry both natively, unlike the prior one, but duplicating
a row still needs them to stay unique manifest-wide) and split forced to
"train" (defensive; native rows already carry split="train" here).

Usage:
  uv run --with google-cloud-storage \
      python oversample_bcfy_calls_production_shaped.py \
      --train gs://wd-transcription-data/sft/dataset_versions/\
20260724-production-shaped-reconstruction/manifests/canonical/train.jsonl \
      --project <gcp-project> \
      --dest-prefix gs://wd-transcription-data/sft/runs/<round-id>/\
audio/bcfy_calls_oversampled \
      --out-train-uri gs://wd-transcription-data/sft/runs/<round-id>/\
manifests/canonical/train.jsonl
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import threading

from google.cloud import storage

logger = logging.getLogger(__name__)

_thread_local = threading.local()

# 8kHz-range threshold: catch 8000 Hz and any nearby narrowband edge cases
# without accidentally bucketing 16000+/22050/44100/96000 as "narrowband".
NARROWBAND_THRESHOLD_HZ = 12000


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


def _native_sample_rate(row: dict) -> int | None:
    source_audio = row.get("source_audio")
    if not isinstance(source_audio, dict):
        return None
    rate = source_audio.get("sample_rate")
    return rate if isinstance(rate, int) else None


def _thread_local_storage_client(project: str | None) -> storage.Client:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = storage.Client(project=project)
        _thread_local.client = client
    return client


def _duplicate_blob_uri(source_uri: str, dup_idx: int, dest_prefix: str) -> str:
    """Build a distinct destination gs:// URI for one duplicate's audio copy.

    Args:
        source_uri: gs:// URI of the native row's audio being duplicated.
        dup_idx: Zero-based index of this duplicate among its siblings.
        dest_prefix: gs:// prefix under which duplicate audio copies are
            written.

    Returns:
        A gs:// URI distinct from source_uri and from every other
        duplicate's URI.
    """
    name = source_uri.rsplit("/", 1)[-1]
    stem, _, suffix = name.rpartition(".")
    return f"{dest_prefix.rstrip('/')}/{stem}_dup{dup_idx}.{suffix}"


def _duplicate_row(row: dict, dup_idx: int, dest_prefix: str) -> dict:
    """Build one duplicate training row with a distinct identity and audio.

    Args:
        row: Native bcfy_calls training row being duplicated.
        dup_idx: Zero-based index of this duplicate among its siblings.
        dest_prefix: gs:// prefix under which duplicate audio copies are
            written.

    Returns:
        A copy of row with example_id/segment_id suffixed for manifest-
        wide uniqueness, split forced to "train", and audio_filepath
        repointed at a not-yet-created per-duplicate GCS copy (see
        _copy_all_duplicate_audio, which must run before the manifest is
        written).
    """
    dup_row = dict(row)
    dup_row["example_id"] = f"{row['example_id']}_dup{dup_idx}"
    dup_row["segment_id"] = f"{row['segment_id']}_dup{dup_idx}"
    dup_row["split"] = "train"
    dup_row["audio_filepath"] = _duplicate_blob_uri(
        row["audio_filepath"], dup_idx, dest_prefix
    )
    return dup_row


def _copy_blob(project: str | None, source_uri: str, dest_uri: str) -> bool:
    """Server-side copy one audio blob to a new distinct GCS object.

    Args:
        project: GCP project used for GCS access.
        source_uri: gs:// URI of the blob to copy.
        dest_uri: gs:// URI of the destination object.

    Returns:
        True on success (including when the destination already exists,
        which makes reruns idempotent); False on failure.
    """
    try:
        client = _thread_local_storage_client(project)
        src_bucket_name, src_blob_name = source_uri[len("gs://") :].split(
            "/", 1
        )
        dst_bucket_name, dst_blob_name = dest_uri[len("gs://") :].split("/", 1)
        dst_bucket = client.bucket(dst_bucket_name)
        if dst_bucket.blob(dst_blob_name).exists():
            return True
        src_bucket = client.bucket(src_bucket_name)
        src_blob = src_bucket.blob(src_blob_name)
        src_bucket.copy_blob(src_blob, dst_bucket, dst_blob_name)
    except (
        Exception
    ) as exc:  # per-copy failures are logged and counted, not fatal
        logger.warning(
            "failed to copy %s -> %s: %s: %s",
            source_uri,
            dest_uri,
            type(exc).__name__,
            exc,
        )
        return False
    else:
        return True


async def _copy_all_duplicate_audio(
    project: str | None,
    copies: list[tuple[str, str]],
    concurrency: int,
) -> int:
    """Copy every (source_uri, dest_uri) pair concurrently.

    Args:
        project: GCP project used for GCS access.
        copies: Pairs of (source gs:// URI, destination gs:// URI).
        concurrency: Maximum concurrent copy operations.

    Returns:
        The number of copies that failed.
    """
    sem = asyncio.Semaphore(concurrency)
    n_done = 0
    n_failed = 0

    async def worker(source_uri: str, dest_uri: str) -> None:
        nonlocal n_done, n_failed
        async with sem:
            ok = await asyncio.to_thread(
                _copy_blob, project, source_uri, dest_uri
            )
            n_done += 1
            if not ok:
                n_failed += 1
            if n_done % 200 == 0:
                logger.info("copied %s/%s", n_done, len(copies))

    await asyncio.gather(*(worker(s, d) for s, d in copies))
    return n_failed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--train",
        required=True,
        help=(
            "Full canonical train.jsonl for the production-shaped "
            "reconstruction (gs:// or local) -- not a per-dataset file; "
            "this script filters dataset.family == 'bcfy_calls' itself."
        ),
    )
    parser.add_argument(
        "--project", default=None, help="GCP project for GCS access"
    )
    parser.add_argument(
        "--dest-prefix",
        required=True,
        help=(
            "gs:// prefix to write each duplicate's server-side audio "
            "copy under. Required because the canonical manifest "
            "validator rejects rows that share an audio_filepath."
        ),
    )
    parser.add_argument(
        "--dup-8khz",
        type=int,
        default=12,
        help=(
            "Duplication factor for native rows at <12kHz sample rate. "
            "Default reflects an unreviewed proposed ~2:1 weighting "
            "toward 8kHz's measured failure severity -- see module "
            "docstring."
        ),
    )
    parser.add_argument(
        "--dup-other",
        type=int,
        default=6,
        help=(
            "Duplication factor for native rows at >=12kHz sample rate. "
            "See --dup-8khz and the module docstring for where these "
            "defaults come from."
        ),
    )
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument(
        "--out-train-uri",
        required=True,
        help="Destination gs:// URI for the resulting canonical train.jsonl.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    client = storage.Client(project=args.project)

    all_rows = read_jsonl(client, args.train)
    bcfy_calls_rows = [row for row in all_rows if _is_bcfy_calls(row)]
    other_rows = [row for row in all_rows if not _is_bcfy_calls(row)]
    logger.info(
        "Loaded %s total rows: %s bcfy_calls (to be replaced with "
        "oversampled duplicates), %s other (unchanged)",
        len(all_rows),
        len(bcfy_calls_rows),
        len(other_rows),
    )

    oversampled_rows: list[dict] = []
    copies: list[tuple[str, str]] = []
    n_narrowband = 0
    n_unmeasured = 0
    for row in bcfy_calls_rows:
        rate = _native_sample_rate(row)
        if rate is None:
            n_unmeasured += 1
        is_narrowband = rate is not None and rate < NARROWBAND_THRESHOLD_HZ
        n_narrowband += is_narrowband
        dup_n = args.dup_8khz if is_narrowband else args.dup_other
        for dup_idx in range(dup_n):
            dup_row = _duplicate_row(row, dup_idx, args.dest_prefix)
            oversampled_rows.append(dup_row)
            copies.append((row["audio_filepath"], dup_row["audio_filepath"]))

    if n_unmeasured:
        logger.warning(
            "%s bcfy_calls rows had no source_audio.sample_rate field, "
            "defaulted to --dup-other",
            n_unmeasured,
        )
    logger.info(
        "Native bcfy_calls rows: %s narrowband (<%sHz, %sx) + %s other (%sx)",
        n_narrowband,
        NARROWBAND_THRESHOLD_HZ,
        args.dup_8khz,
        len(bcfy_calls_rows) - n_narrowband,
        args.dup_other,
    )
    logger.info(
        "After weighted duplication: %s bcfy_calls training rows",
        len(oversampled_rows),
    )

    logger.info("Copying %s duplicate audio objects...", len(copies))
    n_copy_failed = asyncio.run(
        _copy_all_duplicate_audio(args.project, copies, args.concurrency)
    )
    if n_copy_failed:
        msg = (
            f"{n_copy_failed} of {len(copies)} duplicate audio copies "
            "failed; not writing a manifest that references missing "
            "objects. Re-run to retry -- successful copies are skipped "
            "idempotently."
        )
        raise SystemExit(msg)

    merged_rows = other_rows + oversampled_rows
    logger.info(
        "Writing %s total rows (%s unchanged + %s oversampled bcfy_calls) "
        "-> %s",
        len(merged_rows),
        len(other_rows),
        len(oversampled_rows),
        args.out_train_uri,
    )
    write_jsonl(client, merged_rows, args.out_train_uri)
    logger.info("Wrote %s", args.out_train_uri)


if __name__ == "__main__":
    main()
