"""Run-record writers for the SFT pipeline.

Writes:
  config.json   — resolved config, git SHA, dep versions, tuned-model resource name
  wer_summary.{md,json} — target-oriented eval report
"""

from __future__ import annotations

import datetime
import importlib.metadata
import json
import pathlib
import subprocess
import sys
import typing

from gemini_sft import reporting


def _git_sha() -> str:
    """Return HEAD short SHA of the radio-transcription repo, or 'unknown'."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            cwd=str(
                pathlib.Path(__file__).resolve().parent.parent.parent
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
    results_dir: pathlib.Path,
    round_id: str,
    config: dict[str, typing.Any],
) -> dict[str, typing.Any]:
    """Write (or overwrite) results/<round-id>/config.json with resolved run config.

    Always adds written_at, git_sha, and dep_versions to the written JSON.
    """
    config_with_meta = {
        **config,
        "written_at": datetime.datetime.now(datetime.UTC).isoformat(),
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
    results_dir: pathlib.Path,
    round_id: str,
    report: reporting.EvalReport,
) -> tuple[pathlib.Path, pathlib.Path]:
    """Write the JSON and Markdown WER summaries for one round.

    Args:
        results_dir: Local root directory for evaluation results.
        round_id: Stable run identifier used for the output directory.
        report: Structured evaluation report to serialize and render.

    Returns:
        The local JSON summary path followed by the Markdown summary path.

    Raises:
        OSError: If the output directory or either summary cannot be written.
    """
    out_dir = results_dir / round_id
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = reporting.report_to_dict(report)
    markdown = reporting.render_markdown_report(report)
    json_path = out_dir / "wer_summary.json"
    markdown_path = out_dir / "wer_summary.md"
    json_path.write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )
    markdown_path.write_text(markdown, encoding="utf-8")
    return json_path, markdown_path
