"""Generate and score Gemini predictions for transcript review ranking."""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import json
import pathlib
import sys
import tempfile

from common import gcs_utils, gemini_ranking, prompts, ranking

DEFAULT_LOCATION = "us-central1"
DEFAULT_PREFLIGHT_MODEL = "gemini-3.1-pro-preview"
DEFAULT_FULL_MODEL = "gemini-3.5-flash"


def main(argv: list[str] | None = None) -> int:
    """Run the Phase 2 Gemini ranking CLI.

    Args:
        argv: Optional argument list. Defaults to `sys.argv[1:]`.

    Returns:
        Integer process exit code.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and rank Gemini predictions for review.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    preflight = sub.add_parser("preflight", help="Run a small Gemini smoke run")
    _add_artifact_args(preflight)
    _add_vertex_args(preflight)
    preflight.add_argument(
        "--model",
        default=DEFAULT_PREFLIGHT_MODEL,
        help="Gemini model for smoke/preflight predictions.",
    )
    preflight.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows to process.",
    )
    preflight.set_defaults(func=_run_predicting_command)

    run = sub.add_parser("run", help="Run Gemini predictions for ranking")
    _add_artifact_args(run)
    _add_vertex_args(run)
    run.add_argument(
        "--model",
        default=DEFAULT_FULL_MODEL,
        help="Gemini model for full ranking predictions.",
    )
    run.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum rows to process.",
    )
    run.set_defaults(func=_run_predicting_command)

    rank_cache = sub.add_parser("rank-cache", help="Rank from cache only")
    _add_artifact_args(rank_cache)
    rank_cache.add_argument(
        "--model",
        default=DEFAULT_FULL_MODEL,
        help="Gemini model expected in compatible cache entries.",
    )
    rank_cache.set_defaults(func=_run_rank_cache)

    return parser


def _add_artifact_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--review-pool-jsonl", required=True)
    parser.add_argument("--prediction-cache-jsonl", required=True)
    parser.add_argument("--ranked-jsonl", required=True)
    parser.add_argument("--ranked-csv", required=True)
    parser.add_argument("--excluded-jsonl", required=True)


def _add_vertex_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--project", required=True)
    parser.add_argument("--location", default=DEFAULT_LOCATION)


def _run_rank_cache(args: argparse.Namespace) -> int:
    review_rows = _load_jsonl(args.review_pool_jsonl)
    cache_by_audio_id = _load_cache(args.prediction_cache_jsonl)
    _score_and_write(args, review_rows, cache_by_audio_id)
    return 0


def _run_predicting_command(args: argparse.Namespace) -> int:
    review_rows = _load_jsonl(args.review_pool_jsonl)
    if args.limit is not None:
        review_rows = review_rows[: args.limit]
    cache_by_audio_id = _load_cache(args.prediction_cache_jsonl)

    client = gemini_ranking.new_vertex_client(args.project, args.location)
    runner = gemini_ranking.GeminiRankingRunner(
        client,
        model_id=args.model,
    )
    prompt_fp = ranking.prompt_fingerprint(
        prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
    )
    context_policy_fp = ranking.context_policy_fingerprint(
        num_recent_events=ranking.NUM_RECENT_EVENTS,
    )

    new_entries, final_cache = gemini_ranking.run_source_group_predictions(
        review_rows,
        cache_by_audio_id,
        runner,
        prompt_fp=prompt_fp,
        context_policy_fp=context_policy_fp,
        num_recent_events=ranking.NUM_RECENT_EVENTS,
        created_at=_utc_timestamp(),
    )
    _append_cache_entries(args.prediction_cache_jsonl, new_entries)
    _score_and_write(args, review_rows, final_cache)
    return 0


def _score_and_write(
    args: argparse.Namespace,
    review_rows: list[dict[str, object]],
    cache_by_audio_id: dict[str, ranking.PredictionCacheEntry],
) -> None:
    prompt_fp = ranking.prompt_fingerprint(
        prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
    )
    context_policy_fp = ranking.context_policy_fingerprint(
        num_recent_events=ranking.NUM_RECENT_EVENTS,
    )
    ranked_rows, excluded_rows = ranking.score_ranked_rows(
        review_rows,
        cache_by_audio_id,
        model_id=args.model,
        prompt_fp=prompt_fp,
        context_policy_fp=context_policy_fp,
        num_recent_events=ranking.NUM_RECENT_EVENTS,
    )
    _write_jsonl(args.ranked_jsonl, ranked_rows)
    _write_ranked_csv(args.ranked_csv, ranked_rows)
    _write_jsonl(args.excluded_jsonl, excluded_rows)


def _load_jsonl(path: str) -> list[dict[str, object]]:
    return _load_jsonl_with_missing_policy(path, missing_ok=False)


def _load_jsonl_with_missing_policy(
    path: str,
    *,
    missing_ok: bool,
) -> list[dict[str, object]]:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        if missing_ok and not gcs_utils.blob_exists(storage_client, path):
            return []
        return gcs_utils.download_jsonl_manifest(storage_client, path)

    rows: list[dict[str, object]] = []
    local_path = pathlib.Path(path)
    if not local_path.exists():
        if not missing_ok:
            raise FileNotFoundError(f"JSONL path not found: {path}")
        return rows
    with local_path.open(encoding="utf-8") as input_file:
        for raw_line in input_file:
            line = raw_line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _load_cache(path: str) -> dict[str, ranking.PredictionCacheEntry]:
    entries = {}
    for row in _load_jsonl_with_missing_policy(path, missing_ok=True):
        entry = ranking.PredictionCacheEntry(**row)
        entries[entry.audio_segment_id] = entry
    return entries


def _append_cache_entries(
    path: str,
    entries: list[ranking.PredictionCacheEntry],
) -> None:
    if not entries:
        return
    if _is_gcs_uri(path):
        current_rows = _load_jsonl_with_missing_policy(path, missing_ok=True)
        current_rows.extend(dataclasses.asdict(entry) for entry in entries)
        _write_jsonl(path, current_rows)
        return

    local_path = pathlib.Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    with local_path.open("a", encoding="utf-8") as output_file:
        for entry in entries:
            output_file.write(json.dumps(dataclasses.asdict(entry)) + "\n")


def _write_jsonl(path: str, rows: list[dict[str, object]]) -> None:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as temp_file:
            ranking.write_jsonl(temp_file.name, rows)
            temp_file.flush()
            _upload_file(storage_client, path, temp_file.name)
        return

    local_path = pathlib.Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    ranking.write_jsonl(local_path, rows)


def _write_ranked_csv(path: str, rows: list[dict[str, object]]) -> None:
    if _is_gcs_uri(path):
        storage_client = _new_storage_client()
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as temp_file:
            ranking.write_ranked_csv(temp_file.name, rows)
            temp_file.flush()
            _upload_file(storage_client, path, temp_file.name)
        return

    local_path = pathlib.Path(path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    ranking.write_ranked_csv(local_path, rows)


def _upload_file(storage_client: object, gcs_uri: str, local_path: str) -> None:
    bucket_name, blob_path = gcs_utils.parse_gcs_uri(gcs_uri)
    gcs_utils.upload_file_to_blob(
        storage_client,
        bucket_name,
        blob_path,
        local_path,
    )


def _new_storage_client() -> object:
    from google.cloud import storage

    return storage.Client()


def _is_gcs_uri(path: str) -> bool:
    return path.startswith("gs://")


def _utc_timestamp() -> str:
    return (
        datetime.datetime.now(datetime.UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


if __name__ == "__main__":
    sys.exit(main())
