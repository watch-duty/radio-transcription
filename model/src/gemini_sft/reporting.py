"""Shared report contract for Gemini SFT evaluation outputs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from common.scoring import (
    compute_cer,
    compute_wer,
    empty_or_unintelligible_rate,
    keyword_metrics,
)

REPORT_COLUMNS = (
    "target_label",
    "model",
    "wer",
    "cer",
    "keyword_accuracy",
    "empty_or_unintelligible_rate",
    "insertions",
    "deletions",
    "substitutions",
    "total_reference_words",
    "missing_prediction_count",
    "artifacts",
)


@dataclass(frozen=True)
class ReportArtifacts:
    """Artifact URIs that make a target's aggregate metrics reproducible."""

    raw_output_uri: str | None = None
    online_predictions_uri: str | None = None
    normalized_manifest_uri: str | None = None
    summary_json_uri: str | None = None
    summary_markdown_uri: str | None = None


@dataclass(frozen=True)
class TargetMetrics:
    """Metrics and provenance for one evaluated model target."""

    target_label: str
    model: str
    wer: float
    cer: float
    keyword_accuracy: float | None
    empty_or_unintelligible_rate: float
    insertions: int
    deletions: int
    substitutions: int
    total_reference_words: int
    missing_prediction_count: int
    artifacts: ReportArtifacts = field(default_factory=ReportArtifacts)
    keyword_metrics: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvalReport:
    """Structured report rendered to console, Markdown, and JSON."""

    round_id: str
    generated_at: str
    target: TargetMetrics
    metadata: dict[str, Any] = field(default_factory=dict)


def build_target_metrics(
    label: str,
    model: str,
    refs: list[str],
    hyps: list[str],
    normalizer: Any,
    keywords: list[str],
    missing_prediction_count: int = 0,
    artifacts: ReportArtifacts | None = None,
    metadata: dict[str, Any] | None = None,
) -> TargetMetrics:
    """Build canonical metrics for one evaluated target.

    Args:
        label: Stable operator-facing target label.
        model: Publisher model, tuned endpoint, or checkpoint endpoint.
        refs: Ground-truth transcript strings.
        hyps: Model-predicted transcript strings.
        normalizer: Optional JiWER normalizer passed to shared scorers.
        keywords: Keywords to score for occurrence-weighted accuracy.
        missing_prediction_count: Provider rows missing for this target.
        artifacts: Optional artifact URI bundle.
        metadata: Optional target metadata, such as checkpoint id or epoch.

    Returns:
        Canonical target metrics ready for JSON, Markdown, or console output.
    """
    wer_result = compute_wer(refs, hyps, normalizer=normalizer)
    cer_result = compute_cer(refs, hyps, normalizer=normalizer)
    keyword_rows = keyword_metrics(refs, hyps, keywords)
    total_reference_words = (
        int(wer_result["hits"])
        + int(wer_result["substitutions"])
        + int(wer_result["deletions"])
    )
    return TargetMetrics(
        target_label=label,
        model=model,
        wer=float(wer_result["wer"]),
        cer=float(cer_result["cer"]),
        keyword_accuracy=_overall_keyword_accuracy(keyword_rows),
        empty_or_unintelligible_rate=empty_or_unintelligible_rate(hyps),
        insertions=int(wer_result["insertions"]),
        deletions=int(wer_result["deletions"]),
        substitutions=int(wer_result["substitutions"]),
        total_reference_words=total_reference_words,
        missing_prediction_count=missing_prediction_count,
        artifacts=artifacts or ReportArtifacts(),
        keyword_metrics=keyword_rows,
        metadata=metadata or {},
    )


def report_to_dict(report: EvalReport) -> dict[str, Any]:
    """Return a JSON-compatible dictionary for an eval report."""
    return {
        "round_id": report.round_id,
        "generated_at": report.generated_at,
        "columns": list(REPORT_COLUMNS),
        "metadata": report.metadata,
        "target": _target_to_dict(report.target),
    }


def render_markdown_report(report: EvalReport) -> str:
    """Render the shared report as Markdown."""
    lines = [
        f"# Gemini SFT Eval Report - {report.round_id}",
        "",
        f"Generated: {report.generated_at}",
        "",
        _render_target_table(report.target),
        "",
    ]
    return "\n".join(lines)


def render_console_report(report: EvalReport) -> str:
    """Render the shared report for console output."""
    return _render_target_table(report.target)


def _target_to_dict(target: TargetMetrics) -> dict[str, Any]:
    return {
        "target_label": target.target_label,
        "model": target.model,
        "wer": target.wer,
        "cer": target.cer,
        "keyword_accuracy": target.keyword_accuracy,
        "empty_or_unintelligible_rate": (target.empty_or_unintelligible_rate),
        "insertions": target.insertions,
        "deletions": target.deletions,
        "substitutions": target.substitutions,
        "total_reference_words": target.total_reference_words,
        "missing_prediction_count": target.missing_prediction_count,
        "artifacts": _artifacts_to_dict(target.artifacts),
        "keyword_metrics": target.keyword_metrics,
        "metadata": target.metadata,
    }


def _artifacts_to_dict(artifacts: ReportArtifacts) -> dict[str, str]:
    pairs = {
        "raw_output_uri": artifacts.raw_output_uri,
        "online_predictions_uri": artifacts.online_predictions_uri,
        "normalized_manifest_uri": artifacts.normalized_manifest_uri,
        "summary_json_uri": artifacts.summary_json_uri,
        "summary_markdown_uri": artifacts.summary_markdown_uri,
    }
    return {key: value for key, value in pairs.items() if value}


def _render_target_table(target: TargetMetrics) -> str:
    header = "| " + " | ".join(REPORT_COLUMNS) + " |"
    separator = "|" + "|".join("---" for _ in REPORT_COLUMNS) + "|"
    rows = [header, separator, _render_target_row(target)]
    return "\n".join(rows)


def _render_target_row(target: TargetMetrics) -> str:
    row = _target_to_dict(target)
    values = [_format_cell(row[column]) for column in REPORT_COLUMNS]
    return "| " + " | ".join(values) + " |"


def _format_cell(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, dict):
        if not value:
            return "n/a"
        return json.dumps(value, sort_keys=True)
    return str(value)


def _overall_keyword_accuracy(rows: list[dict[str, Any]]) -> float | None:
    total_occurrences = sum(row["occurrences"] for row in rows)
    if total_occurrences == 0:
        return None
    total_correct = sum(row["correctly_identified"] for row in rows)
    return total_correct / total_occurrences * 100
