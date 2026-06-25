#!/usr/bin/env python3
"""Score Gemini transcript-only context probe outputs."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from common import scoring

MODEL_KEY = "gemini_transcript_context"
REF_NORM_KEY = "_score_ref_norm"
PRED_NORM_KEY = "_score_pred_norm"
HISTORY_NORM_KEY = "_score_history_norms"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("jsonl", nargs="+", help="Probe JSONL output files.")
    parser.add_argument(
        "--output-json",
        default="model/research/gemini_transcript_context_length/"
        "score_summary.json",
    )
    parser.add_argument(
        "--output-md",
        default="model/research/gemini_transcript_context_length/"
        "score_summary.md",
    )
    return parser.parse_args()


def load_jsonl(path: str) -> list[dict[str, Any]]:
    """Load JSONL rows from ``path``."""
    rows: list[dict[str, Any]] = []
    with open(path, encoding="utf-8") as handle:
        for raw_line in handle:
            if raw_line.strip():
                rows.append(json.loads(raw_line))
    return rows


def prediction(row: dict[str, Any]) -> str:
    """Return the probe prediction for one row."""
    return str(row.get(f"pred_text_{MODEL_KEY}") or "")


def is_blank_or_unintelligible(text: str) -> bool:
    """Return whether a prediction is empty or the unintelligible marker."""
    stripped = text.strip()
    return not stripped or stripped == "[UNINTELLIGIBLE]"


def prepare_rows(
    rows: list[dict[str, Any]],
    normalizer: Any,
) -> None:
    """Cache normalized scoring strings once per row."""
    for row in rows:
        if REF_NORM_KEY in row:
            continue
        row[REF_NORM_KEY] = normalizer(str(row.get("text", ""))).strip()
        row[PRED_NORM_KEY] = normalizer(prediction(row)).strip()
        row[HISTORY_NORM_KEY] = [
            normalizer(str(item)).strip()
            for item in row.get(f"history_pred_text_{MODEL_KEY}") or []
        ]


def exact_history_match(row: dict[str, Any]) -> bool:
    """Return whether prediction exactly copies a retained history transcript."""
    pred_norm = str(row.get(PRED_NORM_KEY, ""))
    for history_norm in row.get(HISTORY_NORM_KEY, []):
        if history_norm and pred_norm == history_norm:
            return True
    return False


def history_substring_hit(row: dict[str, Any]) -> bool:
    """Return whether prediction contains a retained history transcript."""
    pred_norm = str(row.get(PRED_NORM_KEY, ""))
    for history_norm in row.get(HISTORY_NORM_KEY, []):
        if len(history_norm.split()) >= 2 and history_norm in pred_norm:
            return True
    return False


def scorable_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return rows whose references remain non-empty after normalization."""
    return [row for row in rows if str(row.get(REF_NORM_KEY, "")).strip()]


def summarize_subset(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute WER and safety metrics for one row subset."""
    scored_rows = scorable_rows(rows)
    references = [str(row[REF_NORM_KEY]) for row in scored_rows]
    hypotheses = [str(row[PRED_NORM_KEY]) for row in scored_rows]
    if scored_rows:
        wer = scoring.compute_wer(
            references,
            hypotheses,
        )
        cer = scoring.compute_cer(
            references,
            hypotheses,
        )
    else:
        wer = {
            "wer": None,
            "insertions": 0,
            "deletions": 0,
            "substitutions": 0,
            "hits": 0,
        }
        cer = {"cer": None}
    blanks = sum(is_blank_or_unintelligible(prediction(row)) for row in rows)
    errors = sum(bool(row.get(f"error_{MODEL_KEY}")) for row in rows)
    exact_copies = sum(exact_history_match(row) for row in rows)
    substring_hits = sum(history_substring_hit(row) for row in rows)
    perfect = sum(
        row[REF_NORM_KEY] == row[PRED_NORM_KEY] for row in scored_rows
    )
    return {
        "rows": len(rows),
        "scored_rows": len(scored_rows),
        "wer": wer["wer"],
        "cer": cer["cer"],
        "insertions": wer["insertions"],
        "deletions": wer["deletions"],
        "substitutions": wer["substitutions"],
        "hits": wer["hits"],
        "perfect_match_rate": (
            round(100 * perfect / len(scored_rows), 2) if scored_rows else 0.0
        ),
        "blank_or_unintelligible_count": blanks,
        "blank_or_unintelligible_rate": (
            round(100 * blanks / len(rows), 2) if rows else 0.0
        ),
        "error_count": errors,
        "exact_history_match_count": exact_copies,
        "history_substring_hit_count": substring_hits,
    }


def summarize_file(path: str, normalizer: Any) -> dict[str, Any]:
    """Summarize one probe output file."""
    rows = load_jsonl(path)
    prepare_rows(rows, normalizer)
    by_dataset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_dataset[str(row.get("dataset_name", "unknown"))].append(row)
    first = rows[0] if rows else {}
    return {
        "path": path,
        "mode": first.get("context_mode"),
        "window": first.get("context_window"),
        "overall": summarize_subset(rows),
        "by_dataset": {
            dataset: summarize_subset(dataset_rows)
            for dataset, dataset_rows in sorted(by_dataset.items())
        },
    }


def markdown_table(summary: dict[str, Any]) -> str:
    """Render a compact Markdown score table."""
    lines = [
        "# Gemini Transcript Context Scores",
        "",
        "| arm | rows | scored | WER | CER | exact % | blank % | errors | copies | substr |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summary["results"]:
        overall = item["overall"]
        arm = f"{item['mode']}={item['window']}"
        lines.append(
            "| {arm} | {rows} | {scored} | {wer} | {cer} | {exact} | "
            "{blank} | {errors} | {copies} | {substr} |".format(
                arm=arm,
                rows=overall["rows"],
                scored=overall["scored_rows"],
                wer="" if overall["wer"] is None else f"{overall['wer']:.2f}",
                cer="" if overall["cer"] is None else f"{overall['cer']:.2f}",
                exact=f"{overall['perfect_match_rate']:.2f}",
                blank=f"{overall['blank_or_unintelligible_rate']:.2f}",
                errors=overall["error_count"],
                copies=overall["exact_history_match_count"],
                substr=overall["history_substring_hit_count"],
            )
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    """Entrypoint."""
    args = parse_args()
    normalizer = scoring.build_normalizer()
    summary = {
        "results": [summarize_file(path, normalizer) for path in args.jsonl],
    }
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    output_md = Path(args.output_md)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(markdown_table(summary), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
