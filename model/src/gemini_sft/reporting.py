"""Shared report contract for Gemini SFT evaluation outputs."""

from __future__ import annotations

import dataclasses
import json
import typing

from common import scoring

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


@dataclasses.dataclass(frozen=True)
class ReportArtifacts:
    """Artifact URIs that make a target's aggregate metrics reproducible.

    Attributes:
        raw_output_uri: Raw batch-provider output location, when applicable.
        online_predictions_uri: Online prediction-attempt cache location.
        rolling_history_index_uri: Rolling wave-artifact index location.
        rolling_history_audit_uri: Transcript-free history audit location.
        normalized_manifest_uri: Normalized inference manifest location.
        summary_json_uri: Published JSON summary location.
        summary_markdown_uri: Published Markdown summary location.
    """

    raw_output_uri: str | None = None
    online_predictions_uri: str | None = None
    rolling_history_index_uri: str | None = None
    rolling_history_audit_uri: str | None = None
    normalized_manifest_uri: str | None = None
    summary_json_uri: str | None = None
    summary_markdown_uri: str | None = None


@dataclasses.dataclass(frozen=True)
class TargetMetrics:
    """Metrics and provenance for one evaluated model target.

    Attributes:
        target_label: Stable operator-facing label for the target.
        model: Publisher model, tuned endpoint, or checkpoint endpoint.
        wer: Word error rate percentage.
        cer: Character error rate percentage.
        keyword_accuracy: Occurrence-weighted keyword accuracy percentage.
        empty_or_unintelligible_rate: Percentage of empty or explicitly
            unintelligible hypotheses.
        insertions: Word-level insertion count.
        deletions: Word-level deletion count.
        substitutions: Word-level substitution count.
        total_reference_words: Number of words in the scoring denominator.
        missing_prediction_count: Eval rows without successful predictions.
        artifacts: URIs for reproducible evaluation artifacts.
        keyword_metrics: Per-keyword occurrence and accuracy details.
        metadata: Backend- or target-specific report metadata.
    """

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
    artifacts: ReportArtifacts = dataclasses.field(
        default_factory=ReportArtifacts
    )
    keyword_metrics: list[dict[str, typing.Any]] = dataclasses.field(
        default_factory=list
    )
    metadata: dict[str, typing.Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class EvalReport:
    """Structured report rendered to console, Markdown, and JSON.

    Attributes:
        round_id: Stable identifier for the SFT run.
        generated_at: ISO-formatted report generation timestamp.
        target: Metrics and provenance for the evaluated target.
        metadata: Run-level report metadata.
    """

    round_id: str
    generated_at: str
    target: TargetMetrics
    metadata: dict[str, typing.Any] = dataclasses.field(default_factory=dict)


def build_target_metrics(
    label: str,
    model: str,
    refs: list[str],
    hyps: list[str],
    normalizer: typing.Any,
    keywords: list[str],
    missing_prediction_count: int = 0,
    artifacts: ReportArtifacts | None = None,
    metadata: dict[str, typing.Any] | None = None,
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

    Raises:
        ImportError: If the optional scoring dependency is unavailable.
        ValueError: If reference and hypothesis counts differ.
    """
    wer_result = scoring.compute_wer(refs, hyps, normalizer=normalizer)
    cer_result = scoring.compute_cer(refs, hyps, normalizer=normalizer)
    keyword_rows = scoring.keyword_metrics(refs, hyps, keywords)
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
        empty_or_unintelligible_rate=scoring.empty_or_unintelligible_rate(hyps),
        insertions=int(wer_result["insertions"]),
        deletions=int(wer_result["deletions"]),
        substitutions=int(wer_result["substitutions"]),
        total_reference_words=total_reference_words,
        missing_prediction_count=missing_prediction_count,
        artifacts=artifacts or ReportArtifacts(),
        keyword_metrics=keyword_rows,
        metadata=metadata or {},
    )


def report_to_dict(report: EvalReport) -> dict[str, typing.Any]:
    """Return a JSON-compatible dictionary for an eval report.

    Args:
        report: Structured report to serialize.

    Returns:
        JSON-compatible report data with columns, metadata, and target metrics.
    """
    return {
        "round_id": report.round_id,
        "generated_at": report.generated_at,
        "columns": list(REPORT_COLUMNS),
        "metadata": report.metadata,
        "target": _target_to_dict(report.target),
    }


def render_markdown_report(report: EvalReport) -> str:
    """Render the shared report as Markdown.

    Args:
        report: Structured report to render.

    Returns:
        Markdown containing the run heading and target metrics table.
    """
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
    """Render the shared report for console output.

    Args:
        report: Structured report to render.

    Returns:
        Markdown-style target metrics table suitable for console logging.
    """
    return _render_target_table(report.target)


def _target_to_dict(target: TargetMetrics) -> dict[str, typing.Any]:
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
        "rolling_history_index_uri": artifacts.rolling_history_index_uri,
        "rolling_history_audit_uri": artifacts.rolling_history_audit_uri,
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


def _format_cell(value: typing.Any) -> str:
    """Format a report value for one safe Markdown table cell.

    Args:
        value: Scalar or artifact mapping to render.

    Returns:
        A display string with stable float/empty formatting and escaped table
        delimiters.
    """
    if value is None:
        rendered = "n/a"
    elif isinstance(value, float):
        rendered = f"{value:.2f}"
    elif isinstance(value, dict):
        if not value:
            rendered = "n/a"
        else:
            rendered = json.dumps(value, sort_keys=True)
    else:
        rendered = str(value)
    return _escape_markdown_cell(rendered)


def _escape_markdown_cell(value: str) -> str:
    """Fold line breaks and escape Markdown table delimiters in one cell.

    Args:
        value: Preformatted cell text.

    Returns:
        Single-line text whose pipes cannot split the surrounding table.
    """
    return (
        value.replace("\\r\\n", " ")
        .replace("\\r", " ")
        .replace("\\n", " ")
        .replace("\r\n", " ")
        .replace("\r", " ")
        .replace("\n", " ")
        .replace("|", "\\|")
    )


def _overall_keyword_accuracy(
    rows: list[dict[str, typing.Any]],
) -> float | None:
    total_occurrences = sum(row["occurrences"] for row in rows)
    if total_occurrences == 0:
        return None
    total_correct = sum(row["correctly_identified"] for row in rows)
    return total_correct / total_occurrences * 100
