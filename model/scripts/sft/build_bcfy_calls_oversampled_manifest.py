"""Track 1 + Track 2: merge bcfy_calls train/eval manifests, split by
source_group (recording-level, to avoid leakage), measure each native
training row's real sample rate, and produce a training manifest duplicated
at DIFFERENT rates for 8kHz vs. everything else -- since 8kHz bcfy_calls
segments fail at ~87.5% vs. ~44.9% for 16kHz -- plus a held-out eval
manifest.

No audio augmentation, no new audio files generated -- duplicate rows all
reference the same existing audio_filepath/derived_audio_uri. This is a
manifest-only transformation, chosen to fit a 5-day timeline; see the
proposal doc for the overfitting-risk trade-off this accepts.

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


def _duplicate_row(row: dict, dup_idx: int, rate: int | None) -> dict:
    """Build one duplicate training row isolated from real episode context.

    Args:
        row: Native source row being duplicated.
        dup_idx: Zero-based index of this duplicate among its siblings.
        rate: Measured sample rate in Hz for the source row's audio, or
            None if measurement failed.

    Returns:
        A copy of row with a unique row_index and a synthetic
        original_audio_uri so gemini_sft's prior-context builder treats
        this duplicate as its own singleton episode instead of grouping it
        with the real segments from the same source recording.
    """
    dup_row = dict(row)
    dup_row["row_index"] = f"{row['row_index']}_dup{dup_idx}"
    dup_row["_measured_sample_rate_hz"] = rate
    base_episode_key = row.get("original_audio_uri") or row["audio_filepath"]
    dup_row["original_audio_uri"] = (
        f"{base_episode_key}#oversample_dup{dup_idx}"
    )
    return dup_row


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
    n_narrowband = 0
    for i, row in enumerate(native_train_rows):
        rate = rates.get(i)
        is_narrowband = rate is not None and rate < NARROWBAND_THRESHOLD_HZ
        n_narrowband += is_narrowband
        dup_n = args.dup_8khz if is_narrowband else args.dup_other
        for dup_idx in range(dup_n):
            oversampled_train_rows.append(_duplicate_row(row, dup_idx, rate))

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

    write_jsonl(oversampled_train_rows, args.out_train)
    write_jsonl(new_eval_rows, args.out_eval)
    logger.info("Wrote %s and %s", args.out_train, args.out_eval)


if __name__ == "__main__":
    main()
