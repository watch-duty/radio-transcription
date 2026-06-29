"""Target backend resolution and execution helpers for Gemini SFT eval."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.gcs_utils import (
    blob_exists,
    download_gcs_uri,
    upload_local_file,
)
from common.gemini.context import ContextTurn
from common.gemini import request_identity as request_identity_lib
from common.gemini.vertex import (
    GEMINI_GENERATION_CONFIG,
    GEMINI_SAFETY_SETTINGS,
    build_request,
    genai,
    resource_location,
    types,
)
from gemini_sft.config import EvalExecutionConfig, EvalModelTarget

LOGGER = logging.getLogger(__name__)

ONLINE_RETRY_SLEEP_SECONDS = 2.0
ONLINE_SYNC_EVERY = 100
ONLINE_LOG_EVERY = 100

@dataclass(frozen=True)
class OnlineResumeState:
    rows_by_audio_uri: dict[str, dict[str, Any]]
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


def online_prediction_uri(run_gcs_prefix: str, label: str) -> str:
    return (
        f"{run_gcs_prefix.rstrip('/')}/evals/{label}/online_predictions.jsonl"
    )


def online_prediction_metadata_uri(run_gcs_prefix: str, label: str) -> str:
    return (
        f"{run_gcs_prefix.rstrip('/')}/evals/{label}/"
        "online_predictions.meta.json"
    )


def build_online_request_identity(
    *,
    target_label: str,
    model: str,
    eval_manifest_uri: str,
    audio_uris: Sequence[str],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    generation_config: dict[str, Any],
    safety_settings: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    """Return the durable identity for online prediction reuse."""
    return request_identity_lib.build_request_identity(
        target_label=target_label,
        model=model,
        eval_manifest_uri=eval_manifest_uri,
        audio_uris=audio_uris,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
        generation_config=generation_config,
        safety_settings=safety_settings,
    )


def request_identity_hash(identity: dict[str, Any]) -> str:
    return request_identity_lib.request_identity_hash(identity)


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
        return OnlineResumeState({}, 0, request_identity_hash(request_identity))
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
        rows,
        sum(1 for row in rows.values() if row.get("error")),
        request_identity_hash(existing_identity),
    )


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
    identity = build_online_request_identity(
        target_label=target_label,
        model=target_model,
        eval_manifest_uri=eval_manifest_uri,
        audio_uris=audio_uri_list,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
        generation_config=GEMINI_GENERATION_CONFIG,
        safety_settings=GEMINI_SAFETY_SETTINGS,
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
    missing_audio_uris = [
        audio_uri for audio_uri in audio_uri_list if audio_uri not in completed
    ]
    if not missing_audio_uris:
        return _prediction_map(
            completed,
            predictions_uri=predictions_uri,
            metadata_uri=metadata_uri,
            request_identity_hash=request_identity_hash(identity),
        )

    _require_vertex_sdk()
    request_identity_lib.write_metadata(local_metadata_path, identity)
    upload_local_file(storage_client, local_metadata_path, metadata_uri)
    client = genai.Client(
        vertexai=True,
        project=project,
        location=resource_location(target_model, default_location),
    )
    config = types.GenerateContentConfig(
        system_instruction=system_prompt,
        safety_settings=GEMINI_SAFETY_SETTINGS,
        temperature=float(GEMINI_GENERATION_CONFIG["temperature"]),
        max_output_tokens=int(GEMINI_GENERATION_CONFIG["max_output_tokens"]),
    )
    lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max(1, concurrency))
    progress = {
        "done": len(completed),
        "since_sync": 0,
        "errors": sum(1 for row in completed.values() if row.get("error")),
    }
    max_attempts = max(1, max_retries)

    async def process_one(index: int, audio_uri: str) -> None:
        if audio_uri in completed:
            return
        request = build_request(
            audio_uri,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            history=history_list[index],
            history_mode=prior_context_mode,
        )["request"]
        async with semaphore:
            prediction, error = await _generate_with_retries(
                client=client,
                model_id=target_model,
                contents=request["contents"],
                config=config,
                max_retries=max_attempts,
            )
        out_row = {
            "audio_filepath": audio_uri,
            "pred_text": prediction,
            "error": error,
            "target_label": target_label,
            "model": target_model,
        }
        async with lock:
            completed[audio_uri] = out_row
            _append_prediction(local_predictions_path, out_row)
            progress["done"] += 1
            progress["since_sync"] += 1
            if error:
                progress["errors"] += 1
            if progress["since_sync"] >= ONLINE_SYNC_EVERY:
                upload_local_file(
                    storage_client, local_predictions_path, predictions_uri
                )
                progress["since_sync"] = 0
            if progress["done"] == len(audio_uri_list) or (
                progress["done"] % ONLINE_LOG_EVERY == 0
            ):
                LOGGER.info(
                    "target=%s progress=%s/%s errors=%s",
                    target_label,
                    progress["done"],
                    len(audio_uri_list),
                    progress["errors"],
                )

    await asyncio.gather(
        *(
            process_one(index, audio_uri)
            for index, audio_uri in enumerate(audio_uri_list)
        )
    )
    upload_local_file(storage_client, local_predictions_path, predictions_uri)
    return _prediction_map(
        completed,
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
            row = json.loads(line)
            audio_uri = str(row.get("audio_filepath") or "")
            if audio_uri:
                rows[audio_uri] = row
    return rows

def _append_prediction(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


async def _generate_with_retries(
    *,
    client: Any,
    model_id: str,
    contents: list[dict[str, Any]],
    config: Any,
    max_retries: int,
) -> tuple[str, str | None]:
    last_error = "unknown error"
    for attempt in range(1, max_retries + 1):
        try:
            response = await client.aio.models.generate_content(
                model=model_id,
                contents=contents,
                config=config,
            )
        except Exception as exc:  # pragma: no cover - exercised by callers.
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_retries:
                await asyncio.sleep(ONLINE_RETRY_SLEEP_SECONDS * attempt)
            continue
        text = (response.text or "").strip()
        if text:
            return text, None
        last_error = "empty response"
        if attempt < max_retries:
            await asyncio.sleep(ONLINE_RETRY_SLEEP_SECONDS * attempt)
    return "", last_error


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
            for audio_uri, row in rows.items()
        },
        online_predictions_uri=predictions_uri,
        metadata_uri=metadata_uri,
        error_count=sum(1 for row in rows.values() if row.get("error")),
        request_identity_hash=request_identity_hash,
    )


def _require_vertex_sdk() -> None:
    if genai is None or types is None:
        msg = (
            "google-genai is required for online target inference. "
            "Install the model package with the [vertex] extra."
        )
        raise ImportError(msg)
