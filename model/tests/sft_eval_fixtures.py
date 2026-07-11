"""Shared fixtures for Gemini SFT evaluation tests."""

from __future__ import annotations

import json
import typing

from common.gemini import context, eval_artifacts, request_identity

if typing.TYPE_CHECKING:
    import fake_gcs


def vertex_batch_output(audio_uri: str, text: str) -> str:
    """Build one serialized fake Vertex batch output row.

    Args:
        audio_uri: Audio URI echoed in the fake request payload.
        text: Prediction text returned in the fake response payload.

    Returns:
        A JSON string containing one batch output object.
    """
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
    """Build shared request-identity arguments for batch tests.

    Returns:
        Default context and eval-manifest fields for batch inference helpers.
    """
    return {
        "prior_context_count": 0,
        "prior_context_mode": "text_turns",
        "eval_manifest_uri": "gs://data/eval.jsonl",
    }


def batch_output_uri(run_gcs_prefix: str, label: str = "base") -> str:
    """Build the fake batch output prefix for one eval target.

    Args:
        run_gcs_prefix: Durable GCS prefix for the fake run.
        label: Stable target label used in the artifact path.

    Returns:
        The target-specific batch output GCS prefix.
    """
    return eval_artifacts.eval_target_artifact_paths(
        run_gcs_prefix, label
    ).output_uri


def batch_input_uri(run_gcs_prefix: str, label: str = "base") -> str:
    """Build the fake batch input URI for one eval target.

    Args:
        run_gcs_prefix: Durable GCS prefix for the fake run.
        label: Stable target label used in the artifact path.

    Returns:
        The target-specific batch input JSONL URI.
    """
    return eval_artifacts.eval_target_artifact_paths(
        run_gcs_prefix, label
    ).input_uri


def online_prediction_artifacts(
    run_gcs_prefix: str,
    label: str = "base",
) -> dict[str, str]:
    """Build fake online prediction artifact locations.

    Args:
        run_gcs_prefix: Durable GCS prefix for the fake run.
        label: Stable target label used in the artifact paths.

    Returns:
        Online prediction and request-metadata URIs keyed by constructor name.
    """
    paths = eval_artifacts.eval_target_artifact_paths(run_gcs_prefix, label)
    return {
        "online_predictions_uri": paths.online_predictions_uri,
        "metadata_uri": paths.online_metadata_uri,
    }


def summary_artifacts(run_gcs_prefix: str) -> dict[str, str]:
    """Build fake run-level summary artifact locations.

    Args:
        run_gcs_prefix: Durable GCS prefix for the fake run.

    Returns:
        JSON and Markdown summary URIs keyed by report artifact field.
    """
    summary_json_uri, summary_markdown_uri = (
        eval_artifacts.wer_summary_gcs_uris(run_gcs_prefix)
    )
    return {
        "summary_json_uri": summary_json_uri,
        "summary_markdown_uri": summary_markdown_uri,
    }


def put_batch_metadata(
    storage: fake_gcs.FakeStorageClient,
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
    """Seed fake GCS with a batch request-identity metadata sidecar.

    Args:
        storage: Fake storage client that receives the sidecar.
        run_gcs_prefix: Durable GCS prefix for the fake run.
        label: Stable target label used in the metadata path and identity.
        model: Publisher model ID or full resource recorded in the identity.
        eval_manifest_uri: Canonical eval manifest URI recorded in identity.
        audio_uris: Ordered audio URI sequence represented by the identity.
        system_prompt: System instruction represented by the identity.
        user_prompt: Current-turn user instruction represented by the identity.
        prior_context_count: Context-window size recorded in the identity.
        prior_context_mode: Context encoding mode used for request digests.
        histories: Prior turns aligned one-for-one with ``audio_uris``.

    Returns:
        The GCS URI of the seeded metadata sidecar.

    Raises:
        ValueError: If histories and audio URIs are misaligned or the context
            mode is unsupported.
    """
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
    metadata_uri = eval_artifacts.batch_prediction_metadata_uri(
        run_gcs_prefix, label
    )
    storage.put(
        metadata_uri,
        json.dumps(request_identity.metadata_payload(identity), sort_keys=True)
        + "\n",
    )
    return metadata_uri
