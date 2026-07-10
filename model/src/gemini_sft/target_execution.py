"""Target backend resolution and execution helpers for Gemini SFT eval."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from common.gcs_utils import (
    blob_exists,
    download_gcs_uri,
    upload_local_file,
    upload_text,
)
from common.gemini import request_identity as request_identity_lib
from common.gemini.eval_artifacts import (
    online_prediction_metadata_uri,
    online_prediction_uri,
)
from common.gemini.request_identity import (
    build_gemini_eval_request_identity,
    request_identity_hash,
)
from common.gemini.vertex import (
    GEMINI_GENERATION_CONFIG,
    GEMINI_SAFETY_SETTINGS,
    build_request,
    genai,
    resource_location,
    types,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Sequence
    from pathlib import Path

    from common.gemini.context import ContextTurn

    from gemini_sft.config import EvalExecutionConfig, EvalModelTarget

LOGGER = logging.getLogger(__name__)

ONLINE_SYNC_EVERY = 100
ONLINE_LOG_EVERY = 100


@dataclass(frozen=True)
class OnlineResumeState:
    rows_by_audio_uri: dict[str, dict[str, Any]]
    attempt_rows_by_audio_uri: dict[str, dict[str, Any]]
    error_count: int
    request_identity_hash: str


class OnlinePredictionMap(dict[str, str]):
    """Prediction map plus online artifact locations."""

    def __init__(
        self,
        rows: dict[str, str],
        *,
        online_predictions_uri: str,
        metadata_uri: str,
        error_count: int,
        request_identity_hash: str,
    ) -> None:
        super().__init__(rows)
        self.online_predictions_uri = online_predictions_uri
        self.metadata_uri = metadata_uri
        self.error_count = error_count
        self.request_identity_hash = request_identity_hash


def resolve_target_backend(
    target: EvalModelTarget,
    execution: EvalExecutionConfig,
) -> str:
    """Return the backend to use for an eval target.

    Backend selection is intentionally offline and conservative. Full Vertex
    endpoint resources default to online generation; all other model strings
    default to batch unless the config-wide execution backend forces a choice.
    """
    if execution.backend is not None:
        return execution.backend
    if "/endpoints/" in target.model:
        return "online"
    return "batch"


async def _upload_local_file_async(
    storage_client: Any,
    local_path: Path,
    gcs_uri: str,
) -> None:
    await asyncio.to_thread(
        upload_local_file,
        storage_client,
        local_path,
        gcs_uri,
    )


async def _upload_text_async(
    storage_client: Any,
    text: str,
    gcs_uri: str,
) -> None:
    await asyncio.to_thread(
        upload_text,
        storage_client,
        text,
        gcs_uri,
        content_type="application/jsonl",
    )


async def _upload_periodic_prediction_snapshot(
    *,
    storage_client: Any,
    snapshot: str,
    predictions_uri: str,
    target_label: str,
) -> None:
    try:
        await _upload_text_async(
            storage_client,
            snapshot,
            predictions_uri,
        )
    except Exception as exc:
        LOGGER.warning(
            "target=%s failed to upload periodic online prediction "
            "snapshot to %s: %s",
            target_label,
            predictions_uri,
            exc,
        )


def load_existing_online_predictions(
    *,
    storage_client: Any,
    predictions_uri: str,
    metadata_uri: str,
    local_predictions_path: Path,
    local_metadata_path: Path,
    request_identity: dict[str, Any],
) -> OnlineResumeState:
    """Download and validate reusable online predictions, if present."""
    if not blob_exists(storage_client, predictions_uri):
        local_predictions_path.unlink(missing_ok=True)
        local_metadata_path.unlink(missing_ok=True)
        return OnlineResumeState(
            {},
            {},
            0,
            request_identity_hash(request_identity),
        )
    if not blob_exists(storage_client, metadata_uri):
        msg = (
            "online prediction metadata missing for existing predictions: "
            f"{metadata_uri}"
        )
        raise ValueError(msg)

    download_gcs_uri(storage_client, predictions_uri, local_predictions_path)
    download_gcs_uri(storage_client, metadata_uri, local_metadata_path)
    existing_identity = _load_metadata_identity(local_metadata_path)
    _validate_existing_identity(existing_identity, request_identity)
    rows = _load_prediction_rows(local_predictions_path)
    _validate_existing_rows(rows, existing_identity)
    return OnlineResumeState(
        _successful_prediction_rows(rows),
        rows,
        _online_error_count(rows),
        request_identity_hash(existing_identity),
    )


async def _run_bounded_online_workers(
    *,
    pending_items: Iterable[tuple[int, str]],
    worker_count: int,
    process_one: Callable[[int, str], Awaitable[None]],
) -> None:
    pending = iter(pending_items)

    async def worker() -> None:
        for index, audio_uri in pending:
            await process_one(index, audio_uri)

    await asyncio.gather(*(worker() for _ in range(worker_count)))


async def run_online_target_inference(
    *,
    storage_client: Any,
    run_gcs_prefix: str,
    project: str,
    default_location: str,
    target_label: str,
    target_model: str,
    audio_uris: Sequence[str],
    histories: Sequence[Sequence[ContextTurn]],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    eval_manifest_uri: str,
    local_dir: Path,
    concurrency: int,
    max_retries: int,
) -> OnlinePredictionMap:
    """Run resumable online Gemini inference for one eval target."""
    audio_uri_list = list(audio_uris)
    if len(set(audio_uri_list)) != len(audio_uri_list):
        msg = (
            "duplicate audio_uri in online eval input; cannot map predictions "
            "safely"
        )
        raise ValueError(msg)
    history_list = [list(history) for history in histories]
    if len(history_list) != len(audio_uri_list):
        msg = "histories length must match audio_uris length"
        raise ValueError(msg)

    predictions_uri = online_prediction_uri(run_gcs_prefix, target_label)
    metadata_uri = online_prediction_metadata_uri(run_gcs_prefix, target_label)
    local_predictions_path = (
        local_dir / target_label / "online_predictions.jsonl"
    )
    local_metadata_path = (
        local_dir / target_label / "online_predictions.meta.json"
    )
    identity = build_gemini_eval_request_identity(
        target_label=target_label,
        model=target_model,
        eval_manifest_uri=eval_manifest_uri,
        audio_uris=audio_uri_list,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
        histories=history_list,
    )
    resume = load_existing_online_predictions(
        storage_client=storage_client,
        predictions_uri=predictions_uri,
        metadata_uri=metadata_uri,
        local_predictions_path=local_predictions_path,
        local_metadata_path=local_metadata_path,
        request_identity=identity,
    )
    completed = dict(resume.rows_by_audio_uri)
    attempt_rows = dict(resume.attempt_rows_by_audio_uri)
    missing_audio_uris = [
        audio_uri for audio_uri in audio_uri_list if audio_uri not in completed
    ]
    if not missing_audio_uris:
        return _prediction_map(
            attempt_rows,
            predictions_uri=predictions_uri,
            metadata_uri=metadata_uri,
            request_identity_hash=request_identity_hash(identity),
        )

    _require_vertex_sdk()
    request_identity_lib.write_metadata(local_metadata_path, identity)
    await _upload_local_file_async(
        storage_client,
        local_metadata_path,
        metadata_uri,
    )
    client = genai.Client(
        vertexai=True,
        project=project,
        location=resource_location(target_model, default_location),
    )
    max_attempts = max(1, max_retries)
    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        safety_settings=GEMINI_SAFETY_SETTINGS,
        temperature=float(GEMINI_GENERATION_CONFIG["temperature"]),
        max_output_tokens=int(GEMINI_GENERATION_CONFIG["max_output_tokens"]),
        http_options=types.HttpOptions(
            retry_options=types.HttpRetryOptions(
                attempts=max_attempts,
                initial_delay=2.0,
                max_delay=60.0,
                exp_base=2.0,
                jitter=1.0,
                http_status_codes=[408, 429, 500, 502, 503, 504],
            )
        ),
    )
    lock = asyncio.Lock()
    snapshot_upload_lock = asyncio.Lock()
    batch_size = max(1, concurrency)
    semaphore = asyncio.Semaphore(batch_size)
    progress = {
        "done": len(completed),
        "since_sync": 0,
        "errors": _online_error_count(attempt_rows),
    }

    async def process_one(index: int, audio_uri: str) -> None:
        if audio_uri in completed:
            return
        async with semaphore:
            request = build_request(
                audio_uri,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                history=history_list[index],
                history_mode=prior_context_mode,
            )["request"]
            prediction, error = await _generate_response(
                client=client,
                model_id=target_model,
                contents=request["contents"],
                config=config,
            )
        out_row = {
            "audio_filepath": audio_uri,
            "pred_text": prediction,
            "error": error,
            "target_label": target_label,
            "model": target_model,
        }
        async with lock:
            upload_rows, should_log = _record_online_attempt(
                completed=completed,
                attempt_rows=attempt_rows,
                audio_uri=audio_uri,
                row=out_row,
                local_predictions_path=local_predictions_path,
                progress=progress,
                total_count=len(audio_uri_list),
            )
            log_done = progress["done"]
            log_errors = progress["errors"]
        if should_log:
            LOGGER.info(
                "target=%s progress=%s/%s errors=%s",
                target_label,
                log_done,
                len(audio_uri_list),
                log_errors,
            )
        if upload_rows is not None:
            async with snapshot_upload_lock:
                await _upload_periodic_prediction_snapshot(
                    storage_client=storage_client,
                    snapshot=_prediction_rows_jsonl(upload_rows),
                    predictions_uri=predictions_uri,
                    target_label=target_label,
                )

    await _run_bounded_online_workers(
        pending_items=(
            (index, audio_uri)
            for index, audio_uri in enumerate(audio_uri_list)
            if audio_uri not in completed
        ),
        worker_count=min(batch_size, len(missing_audio_uris)),
        process_one=process_one,
    )
    await _upload_text_async(
        storage_client,
        _prediction_rows_jsonl(attempt_rows.values()),
        predictions_uri,
    )
    return _prediction_map(
        attempt_rows,
        predictions_uri=predictions_uri,
        metadata_uri=metadata_uri,
        request_identity_hash=request_identity_hash(identity),
    )


def _load_metadata_identity(path: Path) -> dict[str, Any]:
    return request_identity_lib.load_metadata_identity(
        path,
        error_message="online prediction request identity mismatch",
    )


def _validate_existing_identity(
    existing_identity: dict[str, Any],
    expected_identity: dict[str, Any],
) -> None:
    request_identity_lib.validate_prefix_identity(
        existing_identity,
        expected_identity,
        "online prediction request identity mismatch",
    )


def _validate_existing_rows(
    rows: dict[str, dict[str, Any]],
    existing_identity: dict[str, Any],
) -> None:
    existing_audio = set(existing_identity.get("audio_uris") or [])
    if all(audio_uri in existing_audio for audio_uri in rows):
        return
    msg = "online prediction request identity mismatch"
    raise ValueError(msg)


def _load_prediction_rows(path: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                LOGGER.warning(
                    "Skipping malformed online prediction row in %s: %s",
                    path,
                    exc,
                )
                continue
            audio_uri = str(row.get("audio_filepath") or "")
            if audio_uri:
                rows[audio_uri] = row
    return rows


def _successful_prediction_rows(
    rows: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        audio_uri: row
        for audio_uri, row in rows.items()
        if not row.get("error")
    }


def _online_error_count(rows: dict[str, dict[str, Any]]) -> int:
    return sum(1 for row in rows.values() if row.get("error"))


def _record_online_attempt(
    *,
    completed: dict[str, dict[str, Any]],
    attempt_rows: dict[str, dict[str, Any]],
    audio_uri: str,
    row: dict[str, Any],
    local_predictions_path: Path,
    progress: dict[str, int],
    total_count: int,
) -> tuple[list[dict[str, Any]] | None, bool]:
    previous_error = bool(attempt_rows.get(audio_uri, {}).get("error"))
    attempt_rows[audio_uri] = row
    if row.get("error"):
        completed.pop(audio_uri, None)
    else:
        completed[audio_uri] = row
    _append_prediction(local_predictions_path, row)
    progress["done"] += 1
    progress["since_sync"] += 1
    if previous_error and not row.get("error"):
        progress["errors"] -= 1
    elif not previous_error and row.get("error"):
        progress["errors"] += 1
    upload_rows = None
    if progress["since_sync"] >= ONLINE_SYNC_EVERY:
        progress["since_sync"] = 0
        upload_rows = list(attempt_rows.values())
    should_log = progress["done"] == total_count or (
        progress["done"] % ONLINE_LOG_EVERY == 0
    )
    return upload_rows, should_log


def _append_prediction(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def _prediction_rows_jsonl(rows: Iterable[dict[str, Any]]) -> str:
    return "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)


async def _generate_response(
    *,
    client: Any,
    model_id: str,
    contents: list[dict[str, Any]],
    config: Any,
) -> tuple[str, str | None]:
    try:
        response = await client.aio.models.generate_content(
            model=model_id,
            contents=contents,
            config=config,
        )
        text = (response.text or "").strip()
    except Exception as exc:
        return "", f"{type(exc).__name__}: {exc}"
    return text, None


def _prediction_map(
    rows: dict[str, dict[str, Any]],
    *,
    predictions_uri: str,
    metadata_uri: str,
    request_identity_hash: str,
) -> OnlinePredictionMap:
    return OnlinePredictionMap(
        {
            audio_uri: str(row.get("pred_text") or "")
            for audio_uri, row in _successful_prediction_rows(rows).items()
        },
        online_predictions_uri=predictions_uri,
        metadata_uri=metadata_uri,
        error_count=_online_error_count(rows),
        request_identity_hash=request_identity_hash,
    )


def _require_vertex_sdk() -> None:
    if genai is None or types is None:
        msg = (
            "google-genai is required for online target inference. "
            "Install the model package with the [vertex] extra."
        )
        raise ImportError(msg)
