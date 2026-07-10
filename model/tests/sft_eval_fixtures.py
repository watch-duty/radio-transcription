from __future__ import annotations

import json
from typing import TYPE_CHECKING

from common.gemini import context, request_identity
from common.gemini.eval_artifacts import (
    batch_prediction_metadata_uri,
    eval_target_artifact_paths,
    wer_summary_gcs_uris,
)

if TYPE_CHECKING:
    from fake_gcs import FakeStorageClient


def vertex_batch_output(audio_uri: str, text: str) -> str:
    return json.dumps(
        {
            "request": {
                "contents": [
                    {
                        "parts": [
                            {
                                "fileData": {
                                    "fileUri": audio_uri,
                                }
                            }
                        ]
                    }
                ]
            },
            "response": {
                "candidates": [{"content": {"parts": [{"text": text}]}}]
            },
        }
    )


def batch_identity_kwargs() -> dict[str, object]:
    return {
        "prior_context_count": 0,
        "prior_context_mode": "text_turns",
        "eval_manifest_uri": "gs://data/eval.jsonl",
    }


def batch_output_uri(run_gcs_prefix: str, label: str = "base") -> str:
    return eval_target_artifact_paths(run_gcs_prefix, label).output_uri


def batch_input_uri(run_gcs_prefix: str, label: str = "base") -> str:
    return eval_target_artifact_paths(run_gcs_prefix, label).input_uri


def online_prediction_artifacts(
    run_gcs_prefix: str,
    label: str = "base",
) -> dict[str, str]:
    paths = eval_target_artifact_paths(run_gcs_prefix, label)
    return {
        "online_predictions_uri": paths.online_predictions_uri,
        "metadata_uri": paths.online_metadata_uri,
    }


def summary_artifacts(run_gcs_prefix: str) -> dict[str, str]:
    summary_json_uri, summary_markdown_uri = wer_summary_gcs_uris(
        run_gcs_prefix
    )
    return {
        "summary_json_uri": summary_json_uri,
        "summary_markdown_uri": summary_markdown_uri,
    }


def put_batch_metadata(
    storage: FakeStorageClient,
    *,
    run_gcs_prefix: str,
    label: str = "base",
    model: str = "gemini-3.1-flash-lite",
    eval_manifest_uri: str = "gs://data/eval.jsonl",
    audio_uris: list[str] | None = None,
    system_prompt: str = "sys",
    user_prompt: str = "user",
    prior_context_count: int = 0,
    prior_context_mode: str = "text_turns",
    histories: list[list[context.ContextTurn]] | None = None,
) -> str:
    kwargs = {
        "target_label": label,
        "model": model,
        "eval_manifest_uri": eval_manifest_uri,
        "audio_uris": audio_uris or ["gs://audio/a.flac"],
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "prior_context_count": prior_context_count,
        "prior_context_mode": prior_context_mode,
    }
    if histories is not None:
        kwargs["histories"] = histories
    identity = request_identity.build_gemini_eval_request_identity(**kwargs)
    metadata_uri = batch_prediction_metadata_uri(run_gcs_prefix, label)
    storage.put(
        metadata_uri,
        json.dumps(request_identity.metadata_payload(identity), sort_keys=True)
        + "\n",
    )
    return metadata_uri
