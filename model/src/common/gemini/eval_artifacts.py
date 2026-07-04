"""Shared GCS artifact paths for Gemini eval runs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EvalTargetArtifactPaths:
    """Durable GCS paths for one Gemini eval run."""

    input_uri: str
    output_uri: str
    batch_metadata_uri: str
    online_predictions_uri: str
    online_metadata_uri: str


def evals_prefix(run_gcs_prefix: str) -> str:
    """Return the run-level eval artifact prefix."""
    return f"{run_gcs_prefix.rstrip('/')}/evals"


def eval_target_prefix(run_gcs_prefix: str, label: str) -> str:
    """Return the eval artifact prefix for one model label."""
    return f"{evals_prefix(run_gcs_prefix)}/{label}"


def eval_target_artifact_paths(
    run_gcs_prefix: str,
    label: str,
) -> EvalTargetArtifactPaths:
    """Return all provider-output paths for one eval model."""
    target_prefix = eval_target_prefix(run_gcs_prefix, label)
    return EvalTargetArtifactPaths(
        input_uri=f"{target_prefix}/input.jsonl",
        output_uri=f"{target_prefix}/output/",
        batch_metadata_uri=f"{target_prefix}/batch_predictions.meta.json",
        online_predictions_uri=f"{target_prefix}/online_predictions.jsonl",
        online_metadata_uri=f"{target_prefix}/online_predictions.meta.json",
    )


def batch_prediction_metadata_uri(run_gcs_prefix: str, label: str) -> str:
    """Return the request metadata sidecar URI for a batch eval run."""
    return eval_target_artifact_paths(run_gcs_prefix, label).batch_metadata_uri


def online_prediction_uri(run_gcs_prefix: str, label: str) -> str:
    """Return the online prediction JSONL URI for one eval run."""
    return eval_target_artifact_paths(
        run_gcs_prefix, label
    ).online_predictions_uri


def online_prediction_metadata_uri(run_gcs_prefix: str, label: str) -> str:
    """Return the online prediction metadata sidecar URI for one eval run."""
    return eval_target_artifact_paths(run_gcs_prefix, label).online_metadata_uri


def wer_summary_gcs_uris(run_gcs_prefix: str) -> tuple[str, str]:
    """Return stable run-level WER summary GCS artifact URIs."""
    prefix = evals_prefix(run_gcs_prefix)
    return (
        f"{prefix}/wer_summary.json",
        f"{prefix}/wer_summary.md",
    )
