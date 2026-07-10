"""Reusable Gemini Vertex batch-inference orchestration."""

from __future__ import annotations

import collections.abc
import json
import logging
import pathlib
import tempfile
import typing

from common import gcs_utils
from common.gemini import eval_artifacts, request_identity, vertex

if typing.TYPE_CHECKING:
    from google.cloud import storage

    from common.gemini import context

logger = logging.getLogger(__name__)

BatchSubmitFn = collections.abc.Callable[..., str]


class BatchPredictionMap(dict[str, str]):
    """Prediction map with its raw Vertex output location.

    Attributes:
        output_uri: GCS prefix containing the raw batch prediction output.
    """

    output_uri: str


def run_batch_audio_inference(
    *,
    storage_client: storage.Client,
    run_gcs_prefix: str,
    gcp_project: str,
    location: str,
    model_id: str,
    label: str,
    audio_uris: collections.abc.Sequence[str],
    system_prompt: str,
    user_prompt: str,
    prior_context_count: int,
    prior_context_mode: str,
    eval_manifest_uri: str,
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.ContextTurn]
    ]
    | None = None,
    submit_fn: BatchSubmitFn = vertex.submit_batch_inference,
) -> BatchPredictionMap | None:
    """Run resumable Gemini batch inference for one evaluation target.

    Args:
        storage_client: Authenticated GCS client used for request and result
            artifacts.
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        gcp_project: GCP project used to submit a new batch job.
        location: Preferred Vertex location for a new batch job.
        model_id: Publisher model ID or full Vertex model resource name.
        label: Stable target label used in artifact paths and logs.
        audio_uris: Ordered audio URIs to transcribe.
        system_prompt: System instruction included in every request.
        user_prompt: User instruction included in every current audio turn.
        prior_context_count: Context-window size recorded in request identity.
        prior_context_mode: Context encoding mode used to build each request.
        eval_manifest_uri: Canonical evaluation manifest URI recorded in
            request identity.
        histories: Prior turns aligned one-for-one with ``audio_uris``. Empty
            histories are used when omitted.
        submit_fn: Batch submission callable, injectable for tests.

    Returns:
        Parsed predictions with their raw output URI, or ``None`` when the
        input has duplicate audio URIs, submission fails, output is missing,
        or output contains duplicate or unexpected audio URIs.

    Raises:
        TypeError: If reusable output metadata lacks an object identity.
        ValueError: If histories are misaligned, the context mode is invalid,
            or existing output metadata is missing or does not match.
    """
    expected_audio_uris = _unique_audio_uris(audio_uris)
    if expected_audio_uris is None:
        logger.error(
            "[%s] eval rows contain duplicate audio_filepath values; one "
            "prediction record cannot belong to multiple manifest rows.",
            label,
        )
        return None
    audio_uri_list = list(audio_uris)
    if histories is None:
        history_list = [[] for _ in audio_uri_list]
    else:
        history_list = [list(history) for history in histories]
    identity = request_identity.build_gemini_eval_request_identity(
        target_label=label,
        model=model_id,
        eval_manifest_uri=eval_manifest_uri,
        audio_uris=audio_uri_list,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prior_context_count=prior_context_count,
        prior_context_mode=prior_context_mode,
        histories=history_list,
    )
    paths = eval_artifacts.eval_target_artifact_paths(run_gcs_prefix, label)
    batch_output_gcs = paths.output_uri
    metadata_uri = paths.batch_metadata_uri
    with tempfile.TemporaryDirectory() as tmp:
        metadata_path = (
            pathlib.Path(tmp) / f"batch_predictions_{label}.meta.json"
        )
        pred_blobs = _list_batch_prediction_blobs(
            storage_client,
            batch_output_gcs,
        )
        if pred_blobs:
            _validate_reusable_batch_output(
                storage_client=storage_client,
                metadata_uri=metadata_uri,
                metadata_path=metadata_path,
                request_identity_payload=identity,
            )
        batch_input_gcs, batch_output_gcs = build_batch_jsonl(
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            label=label,
            audio_uris=audio_uris,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            histories=history_list,
            history_mode=prior_context_mode,
            tmp_dir=pathlib.Path(tmp),
        )
        if pred_blobs:
            preds = _load_batch_predictions(
                storage_client=storage_client,
                output_uri=batch_output_gcs,
                label=label,
                tmp_dir=pathlib.Path(tmp),
                pred_blobs=pred_blobs,
            )
            if preds is None:
                return None
            output_loc = batch_output_gcs
            logger.info(
                "[%s] Reusing existing batch prediction output under %s.",
                label,
                batch_output_gcs,
            )
        else:
            try:
                output_loc = submit_fn(
                    input_uri=batch_input_gcs,
                    output_uri=batch_output_gcs,
                    model=model_id,
                    project=gcp_project,
                    location=location,
                )
            except (RuntimeError, TimeoutError) as exc:
                logger.exception("[%s] Batch inference failed: %s", label, exc)
                return None

            preds = _load_batch_predictions(
                storage_client=storage_client,
                output_uri=output_loc,
                label=label,
                tmp_dir=pathlib.Path(tmp),
            )
            if preds is None:
                return None

        extra_prediction_uris = set(preds) - expected_audio_uris
        if extra_prediction_uris:
            preview = ", ".join(sorted(extra_prediction_uris)[:3])
            logger.error(
                "[%s] prediction output contained audio URIs outside the eval "
                "manifest: %s",
                label,
                preview,
            )
            return None

        if not pred_blobs:
            request_identity.write_metadata(metadata_path, identity)
            gcs_utils.upload_local_file(
                storage_client,
                metadata_path,
                metadata_uri,
            )

    missing = max(0, len(expected_audio_uris) - len(preds))
    if missing > 0:
        logger.warning(
            "[%s] %s/%s unique segments returned no prediction; they score as "
            "full deletions.",
            label,
            missing,
            len(expected_audio_uris),
        )
    preds.output_uri = output_loc
    return preds


def build_batch_jsonl(
    *,
    storage_client: storage.Client,
    run_gcs_prefix: str,
    label: str,
    audio_uris: collections.abc.Sequence[str],
    system_prompt: str,
    user_prompt: str,
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.ContextTurn]
    ]
    | None = None,
    history_mode: str = "text_turns",
    tmp_dir: pathlib.Path,
) -> tuple[str, str]:
    """Write and upload a Vertex batch input JSONL file.

    Args:
        storage_client: Authenticated GCS client used for the upload.
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        label: Stable target label used in the input and output paths.
        audio_uris: Ordered audio URIs to encode as batch requests.
        system_prompt: System instruction included in every request.
        user_prompt: User instruction included in every current audio turn.
        histories: Prior turns aligned one-for-one with ``audio_uris``.
        history_mode: Context encoding mode used to build each request.
        tmp_dir: Existing local directory in which to write the JSONL file.

    Returns:
        A pair containing the uploaded input URI and batch output prefix.

    Raises:
        ValueError: If histories are misaligned, audio URIs are duplicated, or
            ``history_mode`` is unsupported.
    """
    if histories is not None and len(histories) != len(audio_uris):
        msg = "histories must have one entry per audio URI"
        raise ValueError(msg)
    if _unique_audio_uris(audio_uris) is None:
        msg = (
            "duplicate audio_uri in batch eval input; cannot map predictions "
            "safely"
        )
        raise ValueError(msg)
    batch_input_path = tmp_dir / f"batch_input_{label}.jsonl"
    with batch_input_path.open("w", encoding="utf-8") as fh:
        for index, audio_uri in enumerate(audio_uris):
            history = histories[index] if histories is not None else None
            fh.write(
                json.dumps(
                    vertex.build_request(
                        audio_uri,
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        history=history,
                        history_mode=history_mode,
                    )
                )
                + "\n"
            )
    paths = eval_artifacts.eval_target_artifact_paths(run_gcs_prefix, label)
    batch_input_gcs = paths.input_uri
    batch_output_gcs = paths.output_uri
    in_bucket, in_blob = gcs_utils.parse_gcs_uri(batch_input_gcs)
    gcs_utils.upload_file_to_blob(
        storage_client, in_bucket, in_blob, str(batch_input_path)
    )
    return batch_input_gcs, batch_output_gcs


def _load_batch_predictions(
    *,
    storage_client: storage.Client,
    output_uri: str,
    label: str,
    tmp_dir: pathlib.Path,
    pred_blobs: collections.abc.Sequence[typing.Any] | None = None,
    missing_ok: bool = False,
) -> BatchPredictionMap | None:
    """Download and merge prediction shards without ambiguous duplicates.

    Args:
        storage_client: Client used to list and download output blobs.
        output_uri: GCS prefix containing one batch job's output.
        label: Target label included in diagnostics.
        tmp_dir: Local directory used for downloaded shards.
        pred_blobs: Optional prelisted output blobs beneath ``output_uri``.
        missing_ok: Whether absent output should avoid an error log.

    Returns:
        Predictions keyed by audio URI, or ``None`` when output is absent or
        multiple blobs contain the same audio URI.
    """
    pred_blobs = pred_blobs or _list_batch_prediction_blobs(
        storage_client,
        output_uri,
    )
    if not pred_blobs:
        if not missing_ok:
            logger.error(
                "[%s] no .jsonl prediction output under %s.",
                label,
                output_uri,
            )
        return None
    out_bucket, _ = gcs_utils.parse_gcs_uri(output_uri.rstrip("/") + "/")
    preds = BatchPredictionMap()
    for i, blob in enumerate(pred_blobs):
        local_path = tmp_dir / f"predictions_{i}.jsonl"
        gcs_utils.download_blob_to_file(
            storage_client, out_bucket, blob.name, str(local_path)
        )
        with local_path.open(encoding="utf-8") as handle:
            parsed = vertex.parse_batch_output(handle)
        duplicate_audio_uris = set(preds) & set(parsed)
        if duplicate_audio_uris:
            preview = ", ".join(sorted(duplicate_audio_uris)[:3])
            logger.error(
                "[%s] duplicate audio URIs across batch output blobs: %s",
                label,
                preview,
            )
            return None
        preds.update(parsed)
    return preds


def _list_batch_prediction_blobs(
    storage_client: storage.Client,
    output_uri: str,
) -> list[typing.Any]:
    out_bucket, out_prefix = gcs_utils.parse_gcs_uri(
        output_uri.rstrip("/") + "/"
    )
    return [
        blob
        for blob in storage_client.bucket(out_bucket).list_blobs(
            prefix=out_prefix
        )
        if blob.name.endswith(".jsonl")
    ]


def _validate_reusable_batch_output(
    *,
    storage_client: storage.Client,
    metadata_uri: str,
    metadata_path: pathlib.Path,
    request_identity_payload: dict[str, typing.Any],
) -> None:
    if not gcs_utils.blob_exists(storage_client, metadata_uri):
        msg = f"batch prediction metadata missing: {metadata_uri}"
        raise ValueError(msg)
    gcs_utils.download_gcs_uri(storage_client, metadata_uri, metadata_path)
    existing_identity = request_identity.load_metadata_identity(
        metadata_path,
        error_message="batch prediction request identity mismatch",
    )
    request_identity.validate_exact_identity(
        existing_identity,
        request_identity_payload,
        "batch prediction request identity mismatch",
    )


def _unique_audio_uris(
    audio_uris: collections.abc.Sequence[str],
) -> set[str] | None:
    seen: set[str] = set()
    for audio_uri in audio_uris:
        if audio_uri in seen:
            return None
        seen.add(audio_uri)
    return seen
