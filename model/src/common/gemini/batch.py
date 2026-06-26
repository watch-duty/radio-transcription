"""Reusable Gemini Vertex batch-inference orchestration."""

from __future__ import annotations

import json
import logging
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from common.gcs_utils import (
    download_blob_to_file,
    parse_gcs_uri,
    upload_file_to_blob,
)
from common.gemini.context import ContextTurn
from common.gemini.vertex import (
    build_request,
    parse_batch_output,
    submit_batch_inference,
)

if TYPE_CHECKING:
    from google.cloud import storage

logger = logging.getLogger(__name__)

BatchSubmitFn = Callable[..., str]


class BatchPredictionMap(dict[str, str]):
    """Prediction map with the GCS batch output URI attached for provenance."""

    output_uri: str


def run_batch_audio_inference(
    *,
    storage_client: storage.Client,
    run_gcs_prefix: str,
    gcp_project: str,
    location: str,
    model_id: str,
    label: str,
    audio_uris: Sequence[str],
    system_prompt: str,
    user_prompt: str,
    histories: Sequence[Sequence[ContextTurn]] | None = None,
    history_mode: str = "audio",
    submit_fn: BatchSubmitFn = submit_batch_inference,
) -> BatchPredictionMap | None:
    """Run Gemini batch inference for audio URIs and return parsed predictions."""
    expected_audio_uris = _unique_audio_uris(audio_uris)
    if expected_audio_uris is None:
        logger.error(
            "[%s] eval rows contain duplicate audio_filepath values; one "
            "prediction record cannot belong to multiple manifest rows.",
            label,
        )
        return None
    with tempfile.TemporaryDirectory() as tmp:
        batch_input_gcs, batch_output_gcs = build_batch_jsonl(
            storage_client=storage_client,
            run_gcs_prefix=run_gcs_prefix,
            label=label,
            audio_uris=audio_uris,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            histories=histories,
            history_mode=history_mode,
            tmp_dir=Path(tmp),
        )
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
            tmp_dir=Path(tmp),
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
    audio_uris: Sequence[str],
    system_prompt: str,
    user_prompt: str,
    histories: Sequence[Sequence[ContextTurn]] | None = None,
    history_mode: str = "audio",
    tmp_dir: Path,
) -> tuple[str, str]:
    """Write and upload a Vertex batch input JSONL file."""
    if histories is not None and len(histories) != len(audio_uris):
        msg = "histories must have one entry per audio URI"
        raise ValueError(msg)
    batch_input_path = tmp_dir / f"batch_input_{label}.jsonl"
    with batch_input_path.open("w", encoding="utf-8") as fh:
        for index, audio_uri in enumerate(audio_uris):
            history = histories[index] if histories is not None else None
            fh.write(
                json.dumps(
                    build_request(
                        audio_uri,
                        system_prompt=system_prompt,
                        user_prompt=user_prompt,
                        history=history,
                        history_mode=history_mode,
                    )
                )
                + "\n"
            )
    batch_input_gcs = f"{run_gcs_prefix}/evals/{label}/input.jsonl"
    batch_output_gcs = f"{run_gcs_prefix}/evals/{label}/output/"
    in_bucket, in_blob = parse_gcs_uri(batch_input_gcs)
    upload_file_to_blob(
        storage_client, in_bucket, in_blob, str(batch_input_path)
    )
    return batch_input_gcs, batch_output_gcs


def _load_batch_predictions(
    *,
    storage_client: storage.Client,
    output_uri: str,
    label: str,
    tmp_dir: Path,
) -> BatchPredictionMap | None:
    out_bucket, out_prefix = parse_gcs_uri(output_uri.rstrip("/") + "/")
    pred_blobs = [
        blob
        for blob in storage_client.bucket(out_bucket).list_blobs(
            prefix=out_prefix
        )
        if blob.name.endswith(".jsonl")
    ]
    if not pred_blobs:
        logger.error(
            "[%s] no .jsonl prediction output under %s.", label, output_uri
        )
        return None
    preds = BatchPredictionMap()
    for i, blob in enumerate(pred_blobs):
        local_path = tmp_dir / f"predictions_{i}.jsonl"
        download_blob_to_file(
            storage_client, out_bucket, blob.name, str(local_path)
        )
        preds.update(parse_batch_output(local_path.read_text(encoding="utf-8")))
    return preds


def _unique_audio_uris(audio_uris: Sequence[str]) -> set[str] | None:
    seen: set[str] = set()
    for audio_uri in audio_uris:
        if audio_uri in seen:
            return None
        seen.add(audio_uri)
    return seen
