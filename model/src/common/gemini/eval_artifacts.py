"""Shared GCS artifact paths for Gemini eval runs."""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class EvalTargetArtifactPaths:
    """Durable GCS paths for one Gemini evaluation target.

    Attributes:
        input_uri: Batch request JSONL URI.
        output_uri: Prefix for raw batch prediction output.
        batch_metadata_uri: Batch request-identity sidecar URI.
        batch_job_metadata_uri: Submitted batch-job identity sidecar URI.
        online_predictions_uri: Online prediction JSONL URI.
        online_metadata_uri: Online request-identity sidecar URI.
        rolling_history_index_uri: Index of rolling-eval wave artifacts.
        rolling_history_audit_uri: Transcript-free per-row history audit URI.
    """

    input_uri: str
    output_uri: str
    batch_metadata_uri: str
    batch_job_metadata_uri: str
    online_predictions_uri: str
    online_metadata_uri: str
    rolling_history_index_uri: str
    rolling_history_audit_uri: str


def evals_prefix(run_gcs_prefix: str) -> str:
    """Build the run-level evaluation artifact prefix.

    Args:
        run_gcs_prefix: Durable GCS prefix for the prepared run.

    Returns:
        ``run_gcs_prefix`` normalized with one trailing ``/evals`` segment.
    """
    return f"{run_gcs_prefix.rstrip('/')}/evals"


def eval_target_prefix(run_gcs_prefix: str, label: str) -> str:
    """Build the evaluation artifact prefix for one model label.

    Args:
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        label: Stable model target label.

    Returns:
        The target-specific prefix beneath the run's ``evals`` directory.
    """
    return f"{evals_prefix(run_gcs_prefix)}/{label}"


def eval_target_artifact_paths(
    run_gcs_prefix: str,
    label: str,
) -> EvalTargetArtifactPaths:
    """Build all provider-output paths for one evaluation target.

    Args:
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        label: Stable model target label.

    Returns:
        The batch and online artifact path bundle for ``label``.
    """
    target_prefix = eval_target_prefix(run_gcs_prefix, label)
    return EvalTargetArtifactPaths(
        input_uri=f"{target_prefix}/input.jsonl",
        output_uri=f"{target_prefix}/output/",
        batch_metadata_uri=f"{target_prefix}/batch_predictions.meta.json",
        batch_job_metadata_uri=f"{target_prefix}/batch_job.meta.json",
        online_predictions_uri=f"{target_prefix}/online_predictions.jsonl",
        online_metadata_uri=f"{target_prefix}/online_predictions.meta.json",
        rolling_history_index_uri=(
            f"{target_prefix}/rolling_history_index.json"
        ),
        rolling_history_audit_uri=(
            f"{target_prefix}/rolling_history_audit.jsonl"
        ),
    )


def batch_prediction_metadata_uri(run_gcs_prefix: str, label: str) -> str:
    """Build the request metadata sidecar URI for batch evaluation.

    Args:
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        label: Stable model target label.

    Returns:
        The target's batch request-identity sidecar URI.
    """
    return eval_target_artifact_paths(run_gcs_prefix, label).batch_metadata_uri


def online_prediction_uri(run_gcs_prefix: str, label: str) -> str:
    """Build the online prediction JSONL URI for one evaluation target.

    Args:
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        label: Stable model target label.

    Returns:
        The target's online prediction JSONL URI.
    """
    return eval_target_artifact_paths(
        run_gcs_prefix, label
    ).online_predictions_uri


def online_prediction_metadata_uri(run_gcs_prefix: str, label: str) -> str:
    """Build the online prediction metadata URI for one evaluation target.

    Args:
        run_gcs_prefix: Durable GCS prefix for the prepared run.
        label: Stable model target label.

    Returns:
        The target's online request-identity sidecar URI.
    """
    return eval_target_artifact_paths(run_gcs_prefix, label).online_metadata_uri


def wer_summary_gcs_uris(run_gcs_prefix: str) -> tuple[str, str]:
    """Build stable run-level WER summary artifact URIs.

    Args:
        run_gcs_prefix: Durable GCS prefix for the prepared run.

    Returns:
        The JSON summary URI followed by the Markdown summary URI.
    """
    prefix = evals_prefix(run_gcs_prefix)
    return (
        f"{prefix}/wer_summary.json",
        f"{prefix}/wer_summary.md",
    )
