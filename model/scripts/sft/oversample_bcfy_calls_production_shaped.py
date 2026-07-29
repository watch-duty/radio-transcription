"""Oversample bcfy_calls training rows in Shuojing's production-shaped SFT
dataset reconstruction (PR #1127, gs://wd-transcription-data/sft/
dataset_versions/20260724-production-shaped-reconstruction/).

Duplicates every native bcfy_calls training row --dup-factor times,
uniformly. Only training-owned rows are touched -- this script never
reads or writes eval.jsonl/validation.jsonl, per PR #1127's requirement
that any bcfy_calls weighting operate on training-owned rows only.

Each duplicate needs its own real, distinct GCS audio object -- the
canonical manifest validator (common.manifest.validate_canonical_
manifest) rejects two rows sharing an audio_filepath -- so this script
performs a server-side GCS copy_blob per duplicate under --dest-prefix,
and gives each duplicate a unique example_id/segment_id.

The --dup-factor default (3) targets matching bcfy_calls' share of
production audio *duration*, not row count or failure rate -- see
GOO-822: an earlier version of this script weighted 8kHz rows more
heavily than 16kHz rows based on their relative failure severity, but
that skewed bcfy_calls' internal sample-rate mix away from production
reality and overshot its duration share (39% vs. an actual ~18%).
Uniform 3x was verified (against the real canonical manifest and a
duration-weighted production query) to land within half a point of
bcfy_calls' actual production duration share without distorting its
sample-rate mix. Before reusing --dup-factor in a future round,
re-verify against whatever the current canonical manifest and
production distribution look like at that time, rather than assuming 3
still applies.

Usage (from the lightweight ASR docker runtime, which already installs
model[scoring,vertex] -- google-cloud-storage is a core model dependency,
no extra install step needed):
  docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
      bash -lc 'cd model/scripts/sft && python3 \
      oversample_bcfy_calls_production_shaped.py \
      --train gs://wd-transcription-data/sft/dataset_versions/\
20260724-production-shaped-reconstruction/manifests/canonical/train.jsonl \
      --project <gcp-project> \
      --dest-prefix gs://wd-transcription-data/sft/runs/<round-id>/\
audio/bcfy_calls_oversampled \
      --out-train-uri gs://wd-transcription-data/sft/runs/<round-id>/\
manifests/canonical/train.jsonl'

Validating the result: run gemini-sft prepare against --out-train-uri
plus this reconstruction's own, untouched validation/eval manifests:
  validation_manifest_uri = "gs://wd-transcription-data/sft/dataset_versions/\
20260724-production-shaped-reconstruction/manifests/canonical/validation.jsonl"
  eval_manifest_uri = "gs://wd-transcription-data/sft/dataset_versions/\
20260724-production-shaped-reconstruction/manifests/canonical/eval.jsonl"
Use a round_id distinct from --dest-prefix's round-id -- prepare treats
its own sft/runs/<round_id>/ as exclusively owned state and fails
(with an actionable error) if it finds pre-existing files there.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import threading

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage

logger = logging.getLogger(__name__)

_thread_local = threading.local()


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
        try:
            client.bucket(bucket_name).blob(blob_name).upload_from_string(
                text, if_generation_match=0
            )
        except PreconditionFailed as exc:
            msg = (
                f"{path} already exists; refusing to overwrite it. Pick a "
                "fresh --out-train-uri (e.g. a new round-id) instead of "
                "silently overwriting an existing manifest."
            )
            raise SystemExit(msg) from exc
    else:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)


_IMMUTABLE_DATASET_VERSIONS_PREFIX = (
    "gs://wd-transcription-data/sft/dataset_versions/"
)


def _reject_immutable_dataset_aliasing(
    train_uri: str, dest_prefix: str, out_train_uri: str
) -> None:
    """Refuse to run if outputs could alias a published canonical dataset.

    Args:
        train_uri: Source canonical train.jsonl URI (--train).
        dest_prefix: gs:// prefix duplicate audio copies are written under
            (--dest-prefix).
        out_train_uri: Destination URI for the oversampled train.jsonl
            (--out-train-uri).

    Raises:
        SystemExit: If out_train_uri aliases train_uri, or if dest_prefix
            or out_train_uri fall beneath the immutable dataset_versions/
            prefix -- either would risk silently mutating a published
            dataset version while leaving its _SUCCESS marker/publication
            receipt stale.
    """
    if out_train_uri == train_uri:
        msg = (
            "--out-train-uri must not equal --train: writing over the "
            "source manifest risks corrupting a published dataset version."
        )
        raise SystemExit(msg)
    flagged_uris = (
        ("--dest-prefix", dest_prefix),
        ("--out-train-uri", out_train_uri),
    )
    for flag, uri in flagged_uris:
        if uri.startswith(_IMMUTABLE_DATASET_VERSIONS_PREFIX):
            msg = (
                f"{flag}={uri!r} falls beneath the immutable "
                f"{_IMMUTABLE_DATASET_VERSIONS_PREFIX} prefix. This script "
                "must write to an exclusively-owned output namespace (e.g. "
                "gs://wd-transcription-data/sft/runs/<round-id>/), never "
                "back into a published dataset version."
            )
            raise SystemExit(msg)


def _is_bcfy_calls(row: dict) -> bool:
    dataset = row.get("dataset")
    return isinstance(dataset, dict) and dataset.get("family") == "bcfy_calls"


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
        duplicate's URI. Collision-resistant across source blobs that
        happen to share a basename: the destination name folds in the
        source's full object path (bucket-relative), not just its
        basename.
    """
    _, _, bucket_and_object = source_uri.partition("gs://")
    _, _, object_path = bucket_and_object.partition("/")
    stem, _, suffix = object_path.rpartition(".")
    flattened_stem = stem.replace("/", "__")
    return f"{dest_prefix.rstrip('/')}/{flattened_stem}_dup{dup_idx}.{suffix}"


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
        True on success, including a rerun that finds an existing
        destination verified byte-identical to source_uri (making reruns
        idempotent without ever adopting an unverified object); False on
        failure, including an existing destination whose size/md5 do not
        match the source -- that object is never silently adopted as a
        prior copy.
    """
    try:
        client = _thread_local_storage_client(project)
        src_bucket_name, src_blob_name = source_uri[len("gs://") :].split(
            "/", 1
        )
        dst_bucket_name, dst_blob_name = dest_uri[len("gs://") :].split("/", 1)
        src_bucket = client.bucket(src_bucket_name)
        dst_bucket = client.bucket(dst_bucket_name)
        src_blob = src_bucket.blob(src_blob_name)
        try:
            src_bucket.copy_blob(
                src_blob, dst_bucket, dst_blob_name, if_generation_match=0
            )
        except PreconditionFailed:
            src_blob.reload()
            dst_blob = dst_bucket.blob(dst_blob_name)
            dst_blob.reload()
            if (src_blob.size, src_blob.md5_hash) != (
                dst_blob.size,
                dst_blob.md5_hash,
            ):
                logger.warning(
                    "existing %s does not match source %s (size/md5 "
                    "mismatch); refusing to adopt it as a prior copy",
                    dest_uri,
                    source_uri,
                )
                return False
    except Exception as exc:  # per-copy failures are logged/counted, not fatal
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
        "--dup-factor",
        type=int,
        default=3,
        help=(
            "Uniform duplication factor applied to every native bcfy_calls "
            "row. Verified to match bcfy_calls' production duration share "
            "-- see module docstring before reusing it in a future round."
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
    _reject_immutable_dataset_aliasing(
        args.train, args.dest_prefix, args.out_train_uri
    )
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
    for row in bcfy_calls_rows:
        for dup_idx in range(args.dup_factor):
            dup_row = _duplicate_row(row, dup_idx, args.dest_prefix)
            oversampled_rows.append(dup_row)
            copies.append((row["audio_filepath"], dup_row["audio_filepath"]))

    logger.info(
        "After uniform %sx duplication: %s bcfy_calls training rows",
        args.dup_factor,
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
