"""Target backend resolution and execution helpers for Gemini SFT eval."""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import typing

from common import gcs_utils
from common.gemini import context, eval_artifacts, vertex
from common.gemini import request_identity as request_identity_lib

if typing.TYPE_CHECKING:
    import collections.abc
    import pathlib

    from gemini_sft import config

LOGGER = logging.getLogger(__name__)

ONLINE_SYNC_EVERY = 100
ONLINE_LOG_EVERY = 100


@dataclasses.dataclass(frozen=True)
class OnlineResumeState:
    """Validated state restored from an online prediction cache.

    Attributes:
        rows_by_audio_uri: Successful reusable rows keyed by audio URI.
        attempt_rows_by_audio_uri: Latest attempt rows keyed by audio URI.
    """

    rows_by_audio_uri: dict[str, dict[str, typing.Any]]
    attempt_rows_by_audio_uri: dict[str, dict[str, typing.Any]]


class OnlinePredictionMap(dict[str, str]):
    """Successful target predictions plus online artifact locations.

    The mapping stores prediction text keyed by audio URI.

    Attributes:
        online_predictions_uri: GCS location of the stateless prediction cache,
            or ``None`` for predicted-history evaluation.
        metadata_uri: GCS location of stateless request identity metadata, or
            ``None`` for predicted-history evaluation.
        rolling_history_index_uri: GCS location of the predicted-history wave
            index, or ``None`` for stateless evaluation.
        rolling_history_audit_uri: GCS location of transcript-free row audits,
            or ``None`` for stateless evaluation.
        error_count: Number of unresolved prediction errors.
        request_identity_hash: Hash binding the requests that produced these
            results.
    """

    def __init__(
        self,
        rows: dict[str, str],
        *,
        online_predictions_uri: str | None,
        metadata_uri: str | None,
        rolling_history_index_uri: str | None,
        rolling_history_audit_uri: str | None,
        error_count: int,
        request_identity_hash: str,
    ) -> None:
        super().__init__(rows)
        self.online_predictions_uri = online_predictions_uri
        self.metadata_uri = metadata_uri
        self.rolling_history_index_uri = rolling_history_index_uri
        self.rolling_history_audit_uri = rolling_history_audit_uri
        self.error_count = error_count
        self.request_identity_hash = request_identity_hash


class _OnlineWavePredictionMap(OnlinePredictionMap):
    """Internal prediction result retaining attempts needed by later waves.

    Attributes:
        attempt_rows_by_audio_uri: Latest provider attempt for every requested
            audio URI, including unresolved errors.
    """

    def __init__(
        self,
        rows: dict[str, str],
        *,
        online_predictions_uri: str,
        metadata_uri: str,
        error_count: int,
        request_identity_hash: str,
        attempt_rows_by_audio_uri: dict[str, dict[str, typing.Any]],
    ) -> None:
        super().__init__(
            rows,
            online_predictions_uri=online_predictions_uri,
            metadata_uri=metadata_uri,
            rolling_history_index_uri=None,
            rolling_history_audit_uri=None,
            error_count=error_count,
            request_identity_hash=request_identity_hash,
        )
        self.attempt_rows_by_audio_uri = attempt_rows_by_audio_uri


def resolve_target_backend(
    target: config.EvalModelTarget,
    execution: config.EvalExecutionConfig,
    *,
    prior_context_count: int = 0,
) -> str:
    """Return the backend to use for an eval target.

    Backend selection is intentionally offline and conservative. Full Vertex
    endpoint resources default to online generation; all other model strings
    default to batch unless the config-wide execution backend forces a choice.

    Args:
        target: Model target whose resource shape selects the default backend.
        execution: Eval execution controls, including an optional override.
        prior_context_count: Maximum predicted-history turns. Nonzero history
            requires causal online execution.

    Returns:
        ``"online"`` or ``"batch"`` for the target.

    Raises:
        TypeError: If ``prior_context_count`` is not an integer.
        ValueError: If the count is negative or batch is requested with
            predicted history.
    """
    backend = context.resolve_evaluation_backend_for_context(
        prior_context_count,
        execution.backend,
    )
    if backend is not None:
        return backend
    if target.is_endpoint:
        return "online"
    return "batch"


async def _upload_local_file_async(
    storage_client: typing.Any,
    local_path: pathlib.Path,
    gcs_uri: str,
) -> None:
    await asyncio.to_thread(
        gcs_utils.upload_local_file,
        storage_client,
        local_path,
        gcs_uri,
    )


async def _upload_text_async(
    storage_client: typing.Any,
    text: str,
    gcs_uri: str,
) -> None:
    await asyncio.to_thread(
        gcs_utils.upload_text,
        storage_client,
        text,
        gcs_uri,
        content_type="application/jsonl",
    )


async def _upload_json_async(
    storage_client: typing.Any,
    payload: dict[str, typing.Any],
    gcs_uri: str,
) -> None:
    await asyncio.to_thread(
        gcs_utils.upload_text,
        storage_client,
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        gcs_uri,
        content_type="application/json",
    )


async def _upload_periodic_prediction_snapshot(
    *,
    storage_client: typing.Any,
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
    storage_client: typing.Any,
    predictions_uri: str,
    metadata_uri: str,
    local_predictions_path: pathlib.Path,
    local_metadata_path: pathlib.Path,
    request_identity: dict[str, typing.Any],
) -> OnlineResumeState:
    """Download and validate reusable online predictions, if present.

    Args:
        storage_client: Client used to inspect and download GCS artifacts.
        predictions_uri: GCS URI of the online prediction cache.
        metadata_uri: GCS URI of the request identity metadata.
        local_predictions_path: Local destination for downloaded predictions.
        local_metadata_path: Local destination for downloaded metadata.
        request_identity: Identity expected for the current request set.

    Returns:
        Validated successful rows and latest attempts.

    Raises:
        TypeError: If cached request identity metadata has an invalid shape.
        ValueError: If metadata is missing, corrupt, or does not match the
            current request identity.
    """
    if not gcs_utils.blob_exists(storage_client, predictions_uri):
        local_predictions_path.unlink(missing_ok=True)
        local_metadata_path.unlink(missing_ok=True)
        return OnlineResumeState({}, {})
    if not gcs_utils.blob_exists(storage_client, metadata_uri):
        msg = (
            "online prediction metadata missing for existing predictions: "
            f"{metadata_uri}"
        )
        raise ValueError(msg)

    gcs_utils.download_gcs_uri(
        storage_client, predictions_uri, local_predictions_path
    )
    gcs_utils.download_gcs_uri(
        storage_client, metadata_uri, local_metadata_path
    )
    existing_identity = _load_metadata_identity(local_metadata_path)
    _validate_existing_identity(existing_identity, request_identity)
    rows = _load_prediction_rows(local_predictions_path)
    _validate_existing_rows(rows, existing_identity)
    return OnlineResumeState(
        _successful_prediction_rows(rows),
        rows,
    )


async def _run_bounded_online_workers(
    *,
    pending_items: collections.abc.Iterable[tuple[int, str]],
    worker_count: int,
    process_one: collections.abc.Callable[
        [int, str], collections.abc.Awaitable[None]
    ],
) -> None:
    """Process pending items through a fixed-size rolling worker pool.

    Args:
        pending_items: Indexed audio URIs awaiting inference.
        worker_count: Number of concurrent workers to start.
        process_one: Coroutine callback invoked once for each pending item.
    """
    pending = iter(pending_items)

    async def worker() -> None:
        for index, audio_uri in pending:
            await process_one(index, audio_uri)

    tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


async def run_online_target_inference(
    *,
    storage_client: typing.Any,
    run_gcs_prefix: str,
    project: str,
    target_label: str,
    target_model: str,
    segments: collections.abc.Sequence[context.EvaluationSegment],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    eval_manifest_uri: str,
    local_dir: pathlib.Path,
    concurrency: int,
    max_retries: int,
) -> OnlinePredictionMap:
    """Run one target using stateless or causal predicted-history inference.

    This is the only public online-evaluation entry point. Callers supply
    transcript-free segment metadata, never transcript history. With a zero
    context window, all requests are independent and retain the original
    stateless request shape. With a positive window, this function schedules
    causal waves and constructs each history exclusively from the same
    target's earlier successful predictions.

    Args:
        storage_client: Client used to read and publish GCS artifacts.
        run_gcs_prefix: GCS prefix for the durable run artifacts.
        project: Google Cloud project used for Vertex requests.
        target_label: Stable artifact label for the evaluated target.
        target_model: Publisher model or endpoint resource to invoke.
        segments: Transcript-free evaluation segment descriptors.
        system_prompt: System instruction sent with every request.
        user_prompt: Current-audio transcription prompt.
        prior_context_count: Maximum earlier predicted turns per request.
        prior_context_mode: Encoding for predicted transcript history.
        eval_manifest_uri: Canonical eval manifest used by this run.
        local_dir: Directory for local prediction and metadata mirrors.
        concurrency: Maximum concurrent requests within an execution wave.
        max_retries: Maximum SDK attempts for each generation request.

    Returns:
        Successful predictions and mode-appropriate artifact provenance.

    Raises:
        ImportError: If the optional Vertex SDK is unavailable.
        TypeError: If context configuration or cached metadata is invalid.
        ValueError: If segment metadata, context configuration, or cached
            request identity is invalid.
    """
    prior_context_mode = context.validate_evaluation_context_contract(
        prior_context_count,
        prior_context_mode,
    )
    segment_values = tuple(segments)
    if prior_context_count > 0:
        return await _run_predicted_history_target_inference(
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            project=project,
            target_label=target_label,
            target_model=target_model,
            segments=segment_values,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            prior_context_count=prior_context_count,
            prior_context_mode=prior_context_mode,
            eval_manifest_uri=eval_manifest_uri,
            local_dir=local_dir,
            concurrency=concurrency,
            max_retries=max_retries,
        )

    context.build_strict_causal_schedule(segment_values, max_turns=0)
    return await _run_online_wave(
        storage_client=storage_client,
        run_gcs_prefix=run_gcs_prefix,
        project=project,
        target_label=target_label,
        target_model=target_model,
        audio_uris=[segment.audio_uri for segment in segment_values],
        histories=[[] for _ in segment_values],
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
        eval_manifest_uri=eval_manifest_uri,
        local_dir=local_dir,
        concurrency=concurrency,
        max_retries=max_retries,
    )


async def _run_online_wave(
    *,
    storage_client: typing.Any,
    run_gcs_prefix: str,
    project: str,
    target_label: str,
    target_model: str,
    audio_uris: collections.abc.Sequence[str],
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.PredictedHistoryTurn]
    ],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    eval_manifest_uri: str,
    local_dir: pathlib.Path,
    concurrency: int,
    max_retries: int,
) -> _OnlineWavePredictionMap:
    """Run resumable independent Gemini requests for one execution wave.

    Args:
        storage_client: Client used to read and publish GCS artifacts.
        run_gcs_prefix: GCS prefix for the durable run artifacts.
        project: Google Cloud project used for Vertex requests.
        target_label: Stable artifact label for the evaluated target.
        target_model: Publisher model or endpoint resource to invoke.
        audio_uris: Ordered audio URIs to evaluate.
        histories: Predicted-history turns parallel to ``audio_uris``.
        system_prompt: System instruction sent with every request.
        user_prompt: Current-audio transcription prompt.
        prior_context_count: Maximum prior turns encoded in request identity.
        prior_context_mode: Strategy used to encode prior transcript context.
        eval_manifest_uri: Canonical eval manifest used by this run.
        local_dir: Directory for local prediction and metadata mirrors.
        concurrency: Maximum concurrent online generation requests.
        max_retries: Maximum SDK attempts for each generation request.

    Returns:
        Successful predictions with artifact URIs and unresolved error count.

    Raises:
        ImportError: If the optional Vertex SDK is unavailable.
        TypeError: If cached request identity metadata has an invalid shape.
        ValueError: If audio URIs are duplicated, histories are misaligned, or
            cached predictions do not match the current request identity.
    """
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

    predictions_uri = eval_artifacts.online_prediction_uri(
        run_gcs_prefix, target_label
    )
    metadata_uri = eval_artifacts.online_prediction_metadata_uri(
        run_gcs_prefix, target_label
    )
    local_predictions_path = (
        local_dir / target_label / "online_predictions.jsonl"
    )
    local_metadata_path = (
        local_dir / target_label / "online_predictions.meta.json"
    )
    identity = request_identity_lib.build_gemini_eval_request_identity(
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
        return _wave_prediction_map(
            attempt_rows,
            predictions_uri=predictions_uri,
            metadata_uri=metadata_uri,
            request_identity_hash=request_identity_lib.request_identity_hash(
                identity
            ),
        )

    _require_vertex_sdk()
    request_identity_lib.write_metadata(local_metadata_path, identity)
    await _upload_local_file_async(
        storage_client,
        local_metadata_path,
        metadata_uri,
    )
    location = vertex.resource_location(target_model, default="us")
    client = vertex.genai.Client(
        enterprise=True,
        project=project,
        location=location,
    )
    max_attempts = max(1, max_retries)
    retry_http_options = vertex.types.HttpOptions(
        retry_options=vertex.types.HttpRetryOptions(
            attempts=max_attempts,
            initial_delay=2.0,
            max_delay=60.0,
            exp_base=2.0,
            jitter=1.0,
            http_status_codes=[408, 429, 500, 502, 503, 504],
        )
    )
    lock = asyncio.Lock()
    snapshot_upload_lock = asyncio.Lock()
    batch_size = max(1, concurrency)
    progress = {
        "done": len(completed),
        "since_sync": 0,
        "errors": _online_error_count(attempt_rows),
    }

    async def process_one(index: int, audio_uri: str) -> None:
        """Generate and durably record one pending audio prediction.

        Args:
            index: Position of the audio URI and its aligned history.
            audio_uri: GCS URI of the audio segment to transcribe.
        """
        request = vertex.build_request(
            audio_uri,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            history=history_list[index],
            history_mode=prior_context_mode,
        )["request"]
        config = vertex.types.GenerateContentConfig(
            system_instruction=request["systemInstruction"],
            safety_settings=request["safetySettings"],
            http_options=retry_http_options,
            **request["generationConfig"],
        )
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
    return _wave_prediction_map(
        attempt_rows,
        predictions_uri=predictions_uri,
        metadata_uri=metadata_uri,
        request_identity_hash=request_identity_lib.request_identity_hash(
            identity
        ),
    )


async def _run_predicted_history_target_inference(
    *,
    storage_client: typing.Any,
    run_gcs_prefix: str,
    project: str,
    target_label: str,
    target_model: str,
    segments: collections.abc.Sequence[context.EvaluationSegment],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    eval_manifest_uri: str,
    local_dir: pathlib.Path,
    concurrency: int,
    max_retries: int,
) -> OnlinePredictionMap:
    """Evaluate causal waves using only the target's earlier predictions.

    Candidate histories are frozen by the transcript-free causal schedule.
    Failed, missing, blank, or case-insensitive ``[UNINTELLIGIBLE]``
    predictions are omitted in place and never replaced by an older candidate
    or reference.

    Args:
        storage_client: Client used to publish wave and audit artifacts.
        run_gcs_prefix: GCS prefix for the durable run artifacts.
        project: Google Cloud project used for Vertex requests.
        target_label: Stable evaluated-target label.
        target_model: Publisher model or endpoint resource to invoke.
        segments: Transcript-free evaluation segment descriptors.
        system_prompt: System instruction sent with every request.
        user_prompt: Current-audio transcription prompt.
        prior_context_count: Maximum earlier predicted turns per request.
        prior_context_mode: Encoding for predicted transcript history.
        eval_manifest_uri: Canonical eval manifest identity.
        local_dir: Directory for per-wave local mirrors.
        concurrency: Maximum concurrent requests within one causal wave.
        max_retries: Maximum SDK attempts for each generation request.

    Returns:
        Successful predictions plus rolling index and audit artifact URIs.

    Raises:
        ValueError: If the segment schedule is invalid.
    """
    schedule = context.build_strict_causal_schedule(
        segments,
        max_turns=prior_context_count,
    )
    rows_by_wave: dict[int, list[context.RollingHistoryScheduleRow]] = {}
    for row in schedule:
        rows_by_wave.setdefault(row.wave, []).append(row)

    predictions: dict[str, str] = {}
    attempts: dict[str, dict[str, typing.Any]] = {}
    audit_rows: list[dict[str, typing.Any]] = []
    wave_artifacts: list[dict[str, typing.Any]] = []
    for wave in sorted(rows_by_wave):
        wave_rows = rows_by_wave[wave]
        histories = []
        for schedule_row in wave_rows:
            history, audit = _predicted_history_for_schedule_row(
                schedule_row,
                attempts,
            )
            histories.append(history)
            audit_rows.append(audit)
        wave_label = _rolling_wave_label(
            target_label=target_label,
            target_model=target_model,
            wave=wave,
            wave_rows=wave_rows,
            histories=histories,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            prior_context_count=prior_context_count,
            prior_context_mode=prior_context_mode,
            eval_manifest_uri=eval_manifest_uri,
        )
        wave_result = await _run_online_wave(
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            project=project,
            target_label=wave_label,
            target_model=target_model,
            audio_uris=[row.segment.audio_uri for row in wave_rows],
            histories=histories,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            prior_context_count=prior_context_count,
            prior_context_mode=prior_context_mode,
            eval_manifest_uri=eval_manifest_uri,
            local_dir=local_dir / "rolling",
            concurrency=concurrency,
            max_retries=max_retries,
        )
        predictions.update(wave_result)
        attempts.update(wave_result.attempt_rows_by_audio_uri)
        wave_artifacts.append(
            {
                "wave": wave,
                "row_count": len(wave_rows),
                "online_predictions_uri": (wave_result.online_predictions_uri),
                "online_metadata_uri": wave_result.metadata_uri,
                "request_identity_hash": (wave_result.request_identity_hash),
            }
        )

    paths = eval_artifacts.eval_target_artifact_paths(
        run_gcs_prefix,
        target_label,
    )
    schedule_records = [_schedule_record(row) for row in schedule]
    schedule_hash = _json_sha256(schedule_records)
    request_identity_hash = _json_sha256(
        {
            "schedule_sha256": schedule_hash,
            "wave_request_identity_hashes": [
                wave["request_identity_hash"] for wave in wave_artifacts
            ],
        }
    )
    await _upload_text_async(
        storage_client,
        _prediction_rows_jsonl(audit_rows),
        paths.rolling_history_audit_uri,
    )
    await _upload_json_async(
        storage_client,
        {
            "schema_version": 1,
            "target_label": target_label,
            "eval_manifest_uri": eval_manifest_uri,
            "max_history_turns": prior_context_count,
            "row_count": len(schedule),
            "wave_count": len(rows_by_wave),
            "schedule_sha256": schedule_hash,
            "request_identity_hash": request_identity_hash,
            "rolling_history_audit_uri": (paths.rolling_history_audit_uri),
            "waves": wave_artifacts,
        },
        paths.rolling_history_index_uri,
    )
    return OnlinePredictionMap(
        predictions,
        online_predictions_uri=None,
        metadata_uri=None,
        rolling_history_index_uri=paths.rolling_history_index_uri,
        rolling_history_audit_uri=paths.rolling_history_audit_uri,
        error_count=_online_error_count(attempts),
        request_identity_hash=request_identity_hash,
    )


def _predicted_history_for_schedule_row(
    schedule_row: context.RollingHistoryScheduleRow,
    attempts: dict[str, dict[str, typing.Any]],
) -> tuple[list[context.PredictedHistoryTurn], dict[str, typing.Any]]:
    """Resolve one frozen dependency set from prior provider attempts.

    Args:
        schedule_row: Current segment and its preselected causal dependencies.
        attempts: Latest same-target provider attempts keyed by audio URI.

    Returns:
        Usable own-prediction history plus a transcript-free omission audit.
        Missing, errored, blank, and case-insensitive ``[UNINTELLIGIBLE]``
        candidates are omitted in place and never refilled from older rows.
    """
    history: list[context.PredictedHistoryTurn] = []
    omission_counts = {
        "omitted_error_count": 0,
        "omitted_blank_count": 0,
        "omitted_unintelligible_count": 0,
        "omitted_missing_count": 0,
    }
    supplied_prediction_hashes: list[str] = []
    for dependency_uri in schedule_row.dependency_audio_uris:
        attempt = attempts.get(dependency_uri)
        if attempt is None:
            omission_counts["omitted_missing_count"] += 1
            continue
        if attempt.get("error"):
            omission_counts["omitted_error_count"] += 1
            continue
        prediction = str(attempt.get("pred_text") or "").strip()
        if not prediction:
            omission_counts["omitted_blank_count"] += 1
            continue
        if prediction.upper() == "[UNINTELLIGIBLE]":
            omission_counts["omitted_unintelligible_count"] += 1
            continue
        history.append(
            context.PredictedHistoryTurn(
                audio_uri=dependency_uri,
                text=prediction,
            )
        )
        supplied_prediction_hashes.append(_text_sha256(prediction))
    audit = {
        "manifest_index": schedule_row.segment.manifest_index,
        "target_audio_uri_sha256": _text_sha256(schedule_row.segment.audio_uri),
        "wave": schedule_row.wave,
        "candidate_count": len(schedule_row.dependency_audio_uris),
        "supplied_count": len(history),
        "candidate_audio_uri_sha256": [
            _text_sha256(uri) for uri in schedule_row.dependency_audio_uris
        ],
        "supplied_prediction_sha256": supplied_prediction_hashes,
        **omission_counts,
    }
    return history, audit


def _rolling_wave_label(
    *,
    target_label: str,
    target_model: str,
    wave: int,
    wave_rows: collections.abc.Sequence[context.RollingHistoryScheduleRow],
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.PredictedHistoryTurn]
    ],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    eval_manifest_uri: str,
) -> str:
    """Return a versioned artifact label for one realized causal wave.

    The suffix changes whenever recovered upstream predictions alter a
    downstream request. That preserves the earlier wave artifact and avoids
    treating its now-stale request identity as a resumable cache.

    Args:
        target_label: Stable label for the evaluated target.
        target_model: Model resource used for the wave.
        wave: Zero-based causal execution wave.
        wave_rows: Scheduled rows executed in the wave.
        histories: Realized predicted histories aligned with ``wave_rows``.
        system_prompt: System instruction included in each request.
        user_prompt: User instruction included in each request.
        prior_context_count: Maximum number of supplied prior predictions.
        prior_context_mode: Request shape used to encode prior predictions.
        eval_manifest_uri: Canonical evaluation manifest URI.

    Returns:
        A request-bound label suitable for wave artifact paths.
    """
    history_fingerprints = [
        [
            {
                "audio_uri_sha256": _text_sha256(turn.audio_uri),
                "prediction_sha256": _text_sha256(turn.text),
            }
            for turn in history
        ]
        for history in histories
    ]
    digest = _json_sha256(
        {
            "schema_version": 1,
            "target_label": target_label,
            "target_model": target_model,
            "wave": wave,
            "audio_uri_sha256": [
                _text_sha256(row.segment.audio_uri) for row in wave_rows
            ],
            "histories": history_fingerprints,
            "system_prompt_sha256": _text_sha256(system_prompt),
            "user_prompt_sha256": _text_sha256(user_prompt),
            "prior_context_count": prior_context_count,
            "prior_context_mode": prior_context_mode,
            "eval_manifest_uri": eval_manifest_uri,
        }
    )
    return f"{target_label}-rolling-wave-{wave:04d}-{digest[:12]}"


def _schedule_record(
    row: context.RollingHistoryScheduleRow,
) -> dict[str, typing.Any]:
    return {
        "manifest_index": row.segment.manifest_index,
        "target_audio_uri_sha256": _text_sha256(row.segment.audio_uri),
        "dependency_audio_uri_sha256": [
            _text_sha256(uri) for uri in row.dependency_audio_uris
        ],
        "wave": row.wave,
    }


def _text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_sha256(value: typing.Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return _text_sha256(payload)


def _load_metadata_identity(path: pathlib.Path) -> dict[str, typing.Any]:
    return request_identity_lib.load_metadata_identity(
        path,
        error_message="online prediction request identity mismatch",
    )


def _validate_existing_identity(
    existing_identity: dict[str, typing.Any],
    expected_identity: dict[str, typing.Any],
) -> None:
    request_identity_lib.validate_exact_identity(
        existing_identity,
        expected_identity,
        "online prediction request identity mismatch",
    )


def _validate_existing_rows(
    rows: dict[str, dict[str, typing.Any]],
    existing_identity: dict[str, typing.Any],
) -> None:
    existing_audio = set(existing_identity.get("audio_uris") or [])
    if all(audio_uri in existing_audio for audio_uri in rows):
        return
    msg = "online prediction request identity mismatch"
    raise ValueError(msg)


def _load_prediction_rows(
    path: pathlib.Path,
) -> dict[str, dict[str, typing.Any]]:
    """Load the latest valid attempt row for every audio URI.

    Args:
        path: Local append-only online prediction log.

    Returns:
        Parsed rows keyed by audio URI. Later duplicate rows replace earlier
        attempts, and malformed or non-object rows are skipped with a warning.
    """
    rows: dict[str, dict[str, typing.Any]] = {}
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
            if not isinstance(row, dict):
                LOGGER.warning(
                    "Skipping non-object online prediction row in %s",
                    path,
                )
                continue
            audio_uri = str(row.get("audio_filepath") or "")
            if audio_uri:
                rows[audio_uri] = row
    return rows


def _successful_prediction_rows(
    rows: dict[str, dict[str, typing.Any]],
) -> dict[str, dict[str, typing.Any]]:
    return {
        audio_uri: row
        for audio_uri, row in rows.items()
        if not row.get("error")
    }


def _online_error_count(rows: dict[str, dict[str, typing.Any]]) -> int:
    return sum(1 for row in rows.values() if row.get("error"))


def _record_online_attempt(
    *,
    completed: dict[str, dict[str, typing.Any]],
    attempt_rows: dict[str, dict[str, typing.Any]],
    audio_uri: str,
    row: dict[str, typing.Any],
    local_predictions_path: pathlib.Path,
    progress: dict[str, int],
    total_count: int,
) -> tuple[list[dict[str, typing.Any]] | None, bool]:
    """Record one online attempt and advance snapshot/log counters.

    Args:
        completed: Successful rows keyed by audio URI, updated in place.
        attempt_rows: Latest attempt per audio URI, updated in place.
        audio_uri: URI identifying the attempted audio segment.
        row: Prediction or error row produced by the attempt.
        local_predictions_path: Append-only local attempt-log path.
        progress: Mutable progress counters for completion, sync, and errors.
        total_count: Total number of requested audio segments.

    Returns:
        A deduplicated snapshot when the sync threshold is reached and whether
        progress should be logged.
    """
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


def _append_prediction(path: pathlib.Path, row: dict[str, typing.Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def _prediction_rows_jsonl(
    rows: collections.abc.Iterable[dict[str, typing.Any]],
) -> str:
    return "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)


async def _generate_response(
    *,
    client: typing.Any,
    model_id: str,
    contents: list[dict[str, typing.Any]],
    config: typing.Any,
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


def _wave_prediction_map(
    rows: dict[str, dict[str, typing.Any]],
    *,
    predictions_uri: str,
    metadata_uri: str,
    request_identity_hash: str,
) -> _OnlineWavePredictionMap:
    """Build one wave result from its latest provider-attempt rows.

    Args:
        rows: Latest provider-attempt row keyed by audio URI.
        predictions_uri: Durable GCS URI for the wave prediction rows.
        metadata_uri: Durable GCS URI for the wave request identity.
        request_identity_hash: Hash binding the wave result to its requests.

    Returns:
        Successful predictions with wave artifacts and all latest attempts.
    """
    return _OnlineWavePredictionMap(
        {
            audio_uri: str(row.get("pred_text") or "")
            for audio_uri, row in _successful_prediction_rows(rows).items()
        },
        online_predictions_uri=predictions_uri,
        metadata_uri=metadata_uri,
        error_count=_online_error_count(rows),
        request_identity_hash=request_identity_hash,
        attempt_rows_by_audio_uri=dict(rows),
    )


def _require_vertex_sdk() -> None:
    if vertex.genai is None or vertex.types is None:
        msg = (
            "google-genai is required for online target inference. "
            "Install the model package with the [vertex] extra."
        )
        raise ImportError(msg)
