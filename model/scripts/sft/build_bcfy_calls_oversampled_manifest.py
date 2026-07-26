"""Track 1 + Track 2: merge bcfy_calls train/eval manifests, split by
source_group (recording-level, to avoid leakage), measure each native
training row's real sample rate, and produce a training manifest duplicated
at DIFFERENT rates for 8kHz vs. everything else -- since 8kHz bcfy_calls
segments fail at ~87.5% vs. ~44.9% for 16kHz -- plus a held-out eval
manifest.

No audio augmentation -- every duplicate is a plain, unmodified copy of its
source row's audio. Each duplicate still needs its own distinct GCS object,
though: gemini_sft's canonical manifest validator
(common.manifest.validate_canonical_manifest) hard-rejects any row that
shares an audio_filepath with an earlier row in the same manifest
(duplicate_audio_filepath), so simply repeating the same audio_filepath
across duplicate rows fails validation before a run can even be prepared.
This script performs a server-side GCS copy_blob for each duplicate --
cheap, no bytes transferred through this process -- to a distinct object
under --dest-prefix, and points that duplicate's audio_filepath (and the
mirrored derived_audio_uri/model_ready_audio_uri fields) at the copy. See
the proposal doc for the overfitting-risk trade-off this still accepts by
not varying the audio itself.

Duplicate rows are given a unique synthetic original_audio_uri instead of
inheriting the source row's value. gemini_sft's prior-context builder
(common.gemini.context.build_context_histories) groups rows into "episodes"
by original_audio_uri to construct same-recording prior-turn context.
Without this, every duplicate of a row would land in the same episode as
the real distinct segments from that recording, flooding the context
window with repeated identical turns and crowding out genuine prior
context. Giving each duplicate its own synthetic key isolates it into a
singleton episode with empty history instead.

Defaults (--eval-recordings 6, --dup-8khz 12, --dup-other 6) reflect the
agreed starting proposal: hold out enough of the 57-recording pool to
detect a large pre/post effect without sacrificing too much training
volume, and weight duplication roughly proportional to each rate's relative
failure severity within bcfy_calls (~2:1, 8kHz worse). These are being
proposed and acted on directly rather than blocked on a team decision --
flagged for visibility, not gated on sign-off.

Usage:
  uv run --with google-cloud-storage --with soundfile \
      python build_bcfy_calls_oversampled_manifest.py \
      --train gs://BUCKET/.../per_dataset/bcfy_calls/train.jsonl \
      --eval gs://BUCKET/.../per_dataset/bcfy_calls/eval.jsonl \
      --project <gcp-project> \
      --dest-prefix gs://BUCKET/.../audio/derived_oversampled/bcfy_calls \
      --out-train bcfy_calls_oversampled_train.jsonl \
      --out-eval bcfy_calls_holdout_eval.jsonl

Requires google-cloud-storage (manifest + audio access) and soundfile (real
sample-rate measurement); both are optional extras pulled in ad hoc via
``uv run --with`` rather than base package dependencies, since this is a
standalone one-off operator script. Local file paths also work for
--train/--eval, but the audio itself must still be reachable via GCS either
way, since sample rate is measured from the real bytes, not any metadata
field.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import random
import threading
from collections import defaultdict
from pathlib import Path

import soundfile as sf
from google.cloud import storage

logger = logging.getLogger(__name__)

_thread_local = threading.local()

# 8kHz-range threshold: catch 8000 Hz and any nearby narrowband edge cases
# without accidentally bucketing 16000+/22050/44100/96000 as "narrowband".
NARROWBAND_THRESHOLD_HZ = 12000


def read_jsonl(path: str) -> list[dict]:
    """Read a JSONL manifest from a local path or a gs:// URI.

    Args:
        path: Local filesystem path or gs:// URI to a JSONL file.

    Returns:
        The parsed rows in file order.
    """
    if path.startswith("gs://"):
        client = storage.Client()
        bucket_name, blob_name = path[5:].split("/", 1)
        text = client.bucket(bucket_name).blob(blob_name).download_as_text()
    else:
        text = Path(path).read_text()
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def write_jsonl(rows: list[dict], path: str) -> None:
    """Write rows to a local JSONL file, one JSON object per line.

    Args:
        rows: Rows to serialize in order.
        path: Local destination path.
    """
    with open(path, "w") as f:
        f.writelines(json.dumps(row) + "\n" for row in rows)


def _thread_local_storage_client(project: str | None) -> storage.Client:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = storage.Client(project=project)
        _thread_local.client = client
    return client


def _measure_sample_rate(project: str | None, gs_uri: str) -> int | None:
    """Download one audio blob and read its real sample rate.

    Args:
        project: GCP project used for GCS access.
        gs_uri: gs:// URI for the audio blob to measure.

    Returns:
        The measured sample rate in Hz, or None if the blob could not be
        downloaded or decoded.
    """
    try:
        if not gs_uri.startswith("gs://"):
            # Routed through the same broad except below so any row-level
            # failure -- bad URI, network error, decode error -- degrades to
            # a logged warning and a None rate instead of aborting the batch.
            msg = f"expected a gs:// URI, got: {gs_uri}"
            raise ValueError(msg)  # noqa: TRY301
        client = _thread_local_storage_client(project)
        bucket_name, blob_name = gs_uri[5:].split("/", 1)
        raw = client.bucket(bucket_name).blob(blob_name).download_as_bytes()
        with io.BytesIO(raw) as buf:
            info = sf.info(buf)
    except Exception as exc:
        logger.warning(
            "failed to measure sample rate for %s: %s: %s",
            gs_uri,
            type(exc).__name__,
            exc,
        )
        return None
    else:
        return info.samplerate


async def _measure_all_sample_rates(
    project: str | None, rows: list[dict], concurrency: int
) -> dict[int, int | None]:
    """Measure the real sample rate for each row's audio, concurrently.

    Args:
        project: GCP project used for GCS access.
        rows: Native training rows to measure.
        concurrency: Maximum concurrent download/measure operations.

    Returns:
        A mapping from each row's list index to its measured sample rate in
        Hz, or None if measurement failed for that row.
    """
    sem = asyncio.Semaphore(concurrency)
    results: dict[int, int | None] = {}

    async def worker(idx: int, row: dict) -> None:
        async with sem:
            uri = row.get("derived_audio_uri") or row["audio_filepath"]
            rate = await asyncio.to_thread(_measure_sample_rate, project, uri)
            results[idx] = rate
            if len(results) % 100 == 0:
                logger.info("measured %s/%s", len(results), len(rows))

    await asyncio.gather(*(worker(i, row) for i, row in enumerate(rows)))
    return results


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
    name = Path(source_uri[len("gs://") :]).name
    stem, _, suffix = name.rpartition(".")
    return f"{dest_prefix.rstrip('/')}/{stem}_dup{dup_idx}.{suffix}"


def _duplicate_row(
    row: dict, dup_idx: int, rate: int | None, dest_prefix: str
) -> dict:
    """Build one duplicate training row isolated from real episode context.

    Args:
        row: Native source row being duplicated.
        dup_idx: Zero-based index of this duplicate among its siblings.
        rate: Measured sample rate in Hz for the source row's audio, or
            None if measurement failed.
        dest_prefix: gs:// prefix under which duplicate audio copies are
            written.

    Returns:
        A copy of row with a unique row_index, a synthetic
        original_audio_uri so gemini_sft's prior-context builder treats
        this duplicate as its own singleton episode instead of grouping it
        with the real segments from the same source recording, and
        audio_filepath/derived_audio_uri/model_ready_audio_uri repointed at
        a not-yet-created per-duplicate GCS copy (see _copy_all_duplicate_
        audio, which must run before the manifest is written).
    """
    dup_row = dict(row)
    dup_row["row_index"] = f"{row['row_index']}_dup{dup_idx}"
    dup_row["_measured_sample_rate_hz"] = rate
    base_episode_key = row.get("original_audio_uri") or row["audio_filepath"]
    dup_row["original_audio_uri"] = (
        f"{base_episode_key}#oversample_dup{dup_idx}"
    )
    dest_uri = _duplicate_blob_uri(row["audio_filepath"], dup_idx, dest_prefix)
    for field in (
        "audio_filepath",
        "derived_audio_uri",
        "model_ready_audio_uri",
    ):
        if field in dup_row:
            dup_row[field] = dest_uri
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
        "--train", required=True, help="bcfy_calls train.jsonl (gs:// or local)"
    )
    parser.add_argument(
        "--eval", required=True, help="bcfy_calls eval.jsonl (gs:// or local)"
    )
    parser.add_argument(
        "--project", default=None, help="GCP project for GCS access"
    )
    parser.add_argument(
        "--dest-prefix",
        required=True,
        help=(
            "gs:// prefix to write each duplicate's server-side audio copy "
            "under. Required because gemini_sft's canonical manifest "
            "validator rejects rows that share an audio_filepath, so every "
            "duplicate needs its own distinct GCS object."
        ),
    )
    parser.add_argument(
        "--eval-recordings",
        type=int,
        default=6,
        help=(
            "Number of unique source_group recordings to hold out for the "
            "new eval split (default: 6, per the agreed proposal)."
        ),
    )
    parser.add_argument(
        "--dup-8khz",
        type=int,
        default=12,
        help="Duplication factor for native rows measured at <12kHz sample rate.",
    )
    parser.add_argument(
        "--dup-other",
        type=int,
        default=6,
        help="Duplication factor for native rows measured at >=12kHz sample rate.",
    )
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out-train", default="bcfy_calls_oversampled_train.jsonl"
    )
    parser.add_argument("--out-eval", default="bcfy_calls_holdout_eval.jsonl")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()

    train_rows = read_jsonl(args.train)
    eval_rows = read_jsonl(args.eval)
    all_rows = train_rows + eval_rows
    logger.info(
        "Loaded %s train rows + %s eval rows = %s total",
        len(train_rows),
        len(eval_rows),
        len(all_rows),
    )

    by_group: dict[str, list[dict]] = defaultdict(list)
    for row in all_rows:
        by_group[row["source_group"]].append(row)

    groups = sorted(by_group.keys())
    logger.info(
        "%s unique source_group recordings across the combined pool",
        len(groups),
    )

    if args.eval_recordings >= len(groups):
        msg = (
            f"--eval-recordings ({args.eval_recordings}) must be less than "
            f"the total number of recordings ({len(groups)})"
        )
        raise SystemExit(msg)

    # Not used for anything security-sensitive: this is a deterministic,
    # seeded recording-level train/eval split, not a cryptographic operation.
    rng = random.Random(args.seed)  # noqa: S311
    shuffled_groups = groups.copy()
    rng.shuffle(shuffled_groups)

    eval_groups = set(shuffled_groups[: args.eval_recordings])
    train_groups = set(shuffled_groups[args.eval_recordings :])

    new_eval_rows = [row for g in eval_groups for row in by_group[g]]
    native_train_rows = [row for g in train_groups for row in by_group[g]]

    logger.info(
        "Split: %s recordings / %s rows -> train, %s recordings / %s rows -> eval",
        len(train_groups),
        len(native_train_rows),
        len(eval_groups),
        len(new_eval_rows),
    )

    logger.info(
        "Measuring real sample rate for %s native training rows...",
        len(native_train_rows),
    )
    rates = asyncio.run(
        _measure_all_sample_rates(
            args.project, native_train_rows, args.concurrency
        )
    )
    n_failed = sum(1 for r in rates.values() if r is None)
    if n_failed:
        logger.warning(
            "%s rows failed sample-rate measurement, defaulting them to "
            "--dup-other",
            n_failed,
        )

    oversampled_train_rows = []
    copies: list[tuple[str, str]] = []
    n_narrowband = 0
    for i, row in enumerate(native_train_rows):
        rate = rates.get(i)
        is_narrowband = rate is not None and rate < NARROWBAND_THRESHOLD_HZ
        n_narrowband += is_narrowband
        dup_n = args.dup_8khz if is_narrowband else args.dup_other
        for dup_idx in range(dup_n):
            dup_row = _duplicate_row(row, dup_idx, rate, args.dest_prefix)
            oversampled_train_rows.append(dup_row)
            copies.append((row["audio_filepath"], dup_row["audio_filepath"]))

    logger.info(
        "Native rows: %s narrowband (<%sHz, %sx) + %s other (%sx)",
        n_narrowband,
        NARROWBAND_THRESHOLD_HZ,
        args.dup_8khz,
        len(native_train_rows) - n_narrowband,
        args.dup_other,
    )
    logger.info(
        "After weighted duplication: %s training rows",
        len(oversampled_train_rows),
    )

    # Every duplicate needs its own real GCS object -- gemini_sft's
    # canonical manifest validator rejects rows that share an
    # audio_filepath, so this must run and succeed before the manifest is
    # written, not after.
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

    write_jsonl(oversampled_train_rows, args.out_train)
    write_jsonl(new_eval_rows, args.out_eval)
    logger.info("Wrote %s and %s", args.out_train, args.out_eval)


if __name__ == "__main__":
    main()
