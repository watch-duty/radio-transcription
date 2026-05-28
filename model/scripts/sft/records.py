"""Run-record writers for the SFT pipeline.

Writes:
  config.json   — resolved config, git SHA, dep versions, tuned-model resource name
  wer_summary.{md,json} — base vs. tuned WER + full scoring panel (D-15)
  ledger.md     — appended one-row summary per eval run (PIPE-07)
"""

from __future__ import annotations

import importlib.metadata
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _git_sha() -> str:
    """Return HEAD short SHA of the radio-transcription repo, or 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=str(
                Path(__file__).resolve().parent.parent.parent
            ),  # resolves to model/ (inside the repo; git finds the root from here)
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except Exception:
        return "unknown"


def _dep_versions() -> dict[str, str]:
    """Return installed versions of key SFT pipeline deps."""
    packages = [
        "google-genai",
        "google-cloud-storage",
        "jiwer",
        "nemo_text_processing",
        "datasets",
        "torchaudio",
        "soundfile",
    ]
    versions: dict[str, str] = {"python": sys.version.split()[0]}
    for pkg in packages:
        try:
            versions[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            versions[pkg] = "not-installed"
    return versions


def write_config(
    results_dir: Path, round_id: str, config: dict[str, Any]
) -> dict[str, Any]:
    """Write (or overwrite) results/<round-id>/config.json with resolved run config.

    Always adds written_at, git_sha, and dep_versions to the written JSON.
    PIPE-07: config.json records resolved config, git SHA, dep versions, and
    tuned-model resource name (job_name / endpoint).
    """
    config_with_meta = {
        **config,
        "written_at": datetime.now(UTC).isoformat(),
        "git_sha": _git_sha(),
        "dep_versions": _dep_versions(),
    }
    path = results_dir / round_id / "config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(config_with_meta, indent=2, default=str),
        encoding="utf-8",
    )
    return config_with_meta


def write_wer_summary(
    results_dir: Path, round_id: str, metrics: dict[str, Any]
) -> None:
    """Write results/<round-id>/wer_summary.{json,md}.

    D-15: full panel — WER, CER, ins/del/sub, empty/halluc rate, duration buckets,
    keyword accuracy, bootstrap_paired CI. Degrades gracefully when tuned metrics absent.
    """
    out_dir = results_dir / round_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "wer_summary.json").write_text(
        json.dumps(metrics, indent=2, default=str),
        encoding="utf-8",
    )
    (out_dir / "wer_summary.md").write_text(
        _render_wer_md(metrics), encoding="utf-8"
    )


def _render_wer_md(metrics: dict[str, Any]) -> str:
    """Render the wer_summary.md markdown table."""
    has_tuned = "tuned_wer" in metrics and metrics["tuned_wer"] is not None
    lines: list[str] = [
        f"# WER Summary — Round {metrics.get('round_id', 'unknown')}",
        f"Generated: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
        "## Headline Metrics",
        "",
        "| Metric | Base | Tuned | Delta |",
        "|--------|------|-------|-------|",
    ]
    base_wer = metrics.get("base_wer")
    tuned_wer = metrics.get("tuned_wer")
    base_cer = metrics.get("base_cer")
    tuned_cer = metrics.get("tuned_cer")

    def _fmt(v: float | None) -> str:
        return f"{v:.2f}%" if v is not None else "—"

    def _delta(b: float | None, t: float | None) -> str:
        if b is None or t is None:
            return "—"
        return f"{t - b:+.2f}pp"

    lines.append(
        f"| WER | {_fmt(base_wer)} | {_fmt(tuned_wer)} | {_delta(base_wer, tuned_wer)} |"
    )
    lines.append(
        f"| CER | {_fmt(base_cer)} | {_fmt(tuned_cer)} | {_delta(base_cer, tuned_cer)} |"
    )

    # Ins/del/sub breakdown
    for model_key, prefix in [("base", "base"), ("tuned", "tuned")]:
        ins = metrics.get(f"{prefix}_insertions")
        if ins is not None:
            lines += [
                "",
                f"### {model_key.title()} Model — Error Breakdown",
                f"- Insertions: {ins:.2f}%",
                f"- Deletions:  {metrics.get(f'{prefix}_deletions', 0.0):.2f}%",
                f"- Substitutions: {metrics.get(f'{prefix}_substitutions', 0.0):.2f}%",
                f"- Empty/Hallucinated rate: {metrics.get(f'{prefix}_empty_rate', 0.0):.2f}%",
            ]

    base_keywords = metrics.get("base_keyword_metrics") or []
    tuned_keywords = metrics.get("tuned_keyword_metrics") or []
    if base_keywords or tuned_keywords:
        base_by_keyword = {row["keyword"]: row for row in base_keywords}
        tuned_by_keyword = {row["keyword"]: row for row in tuned_keywords}
        ordered_keywords = sorted(
            set(base_by_keyword) | set(tuned_by_keyword),
            key=lambda kw: (
                -max(
                    base_by_keyword.get(kw, {}).get("occurrences", 0),
                    tuned_by_keyword.get(kw, {}).get("occurrences", 0),
                ),
                kw,
            ),
        )

        def _kw_pct(row: dict[str, Any] | None) -> str:
            if not row:
                return "—"
            return _fmt(row.get("accuracy"))

        lines += [
            "",
            "## Keyword Accuracy",
            "",
            f"- Base overall: {_fmt(metrics.get('base_keyword_accuracy'))}",
        ]
        if has_tuned:
            lines.append(
                f"- Tuned overall: {_fmt(metrics.get('tuned_keyword_accuracy'))}"
            )
        lines += [
            "",
            "| Keyword | Occurrences | Base Accuracy | Tuned Accuracy |",
            "|---------|-------------|---------------|----------------|",
        ]
        for keyword in ordered_keywords:
            base_row = base_by_keyword.get(keyword)
            tuned_row = tuned_by_keyword.get(keyword)
            occurrences = max(
                base_row.get("occurrences", 0) if base_row else 0,
                tuned_row.get("occurrences", 0) if tuned_row else 0,
            )
            lines.append(
                f"| {keyword} | {occurrences} | {_kw_pct(base_row)} | {_kw_pct(tuned_row)} |"
            )

    # Bootstrap significance (only when both models present)
    if has_tuned and "bootstrap_p_value" in metrics:
        p = metrics["bootstrap_p_value"]
        ci_low = metrics.get("bootstrap_ci_low", 0.0)
        ci_high = metrics.get("bootstrap_ci_high", 0.0)
        sig = "significant (p < 0.05)" if p < 0.05 else "not significant"
        lines += [
            "",
            "## Bootstrap Paired Test (WER delta significance)",
            f"- p-value: {p:.4f} ({sig})",
            f"- 95% CI: [{ci_low:+.2f}pp, {ci_high:+.2f}pp]",
        ]

    # Duration buckets (if present)
    if "duration_buckets" in metrics:
        buckets = metrics["duration_buckets"]
        if buckets:
            lines += [
                "",
                "## WER by Duration Bucket",
                "",
                "| Bucket | Base WER | Tuned WER |",
                "|--------|----------|-----------|",
            ]
            for bucket_entry in buckets:
                if isinstance(bucket_entry, dict):
                    b_label = bucket_entry.get("bucket", "—")
                    b_wer = _fmt(bucket_entry.get("base_wer"))
                    t_wer = (
                        _fmt(bucket_entry.get("tuned_wer"))
                        if has_tuned
                        else "—"
                    )
                    lines.append(f"| {b_label} | {b_wer} | {t_wer} |")

    lines.append("")
    if not has_tuned:
        lines.append(
            "_Note: Base-only run (no tuned model). Tuned metrics will appear after `tune` + `eval`._"
        )
    return "\n".join(lines) + "\n"


def append_ledger(results_dir: Path, row: dict[str, Any]) -> None:
    """Append one row to results/ledger.md (creates header on first write).

    Row fields: round_id, datasets, base_model, epochs, base_wer, tuned_wer (optional),
    delta (optional), git_sha, timestamp.
    PIPE-07: appended one row per completed eval run.
    """
    ledger_path = results_dir / "ledger.md"
    header = (
        "| round_id | datasets | base_model | epochs | base_wer | tuned_wer | delta | git_sha | timestamp |\n"
        "|----------|----------|------------|--------|----------|-----------|-------|---------|----------|\n"
    )
    if not ledger_path.exists():
        ledger_path.write_text(
            "# SFT Pipeline Run Ledger\n\n" + header, encoding="utf-8"
        )

    def _f(v: float | None, pct: bool = True) -> str:
        if v is None:
            return "—"
        return f"{v:.2f}%" if pct else str(v)

    base_wer = row.get("base_wer")
    tuned_wer = row.get("tuned_wer")
    delta = (
        (tuned_wer - base_wer)
        if (base_wer is not None and tuned_wer is not None)
        else None
    )

    md_row = (
        f"| {row.get('round_id', '—')} "
        f"| {','.join(row.get('datasets') or [])} "
        f"| {row.get('base_model', '—')} "
        f"| {row.get('epochs', '—')} "
        f"| {_f(base_wer)} "
        f"| {_f(tuned_wer)} "
        f"| {_f(delta)} "
        f"| {row.get('git_sha', '—')} "
        f"| {row.get('timestamp', datetime.now(UTC).strftime('%Y-%m-%d'))} |\n"
    )
    with ledger_path.open("a", encoding="utf-8") as f:
        f.write(md_row)
