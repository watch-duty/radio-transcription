"""Render reviewer-facing tables from publication-safe aggregate JSON only."""

# ruff: noqa: INP001

from __future__ import annotations

import argparse
import json
import pathlib
import typing

ROOT = pathlib.Path(__file__).resolve().parents[2]
RESULTS = ROOT / "experiments" / "results"

TARGETS: typing.Final = (
    "base",
    "no_context_sft",
    "prior_context_sft",
)
TARGET_DISPLAY: typing.Final = {
    "base": "Untuned base model",
    "no_context_sft": "History-free SFT",
    "prior_context_sft": "Prior-transcript SFT",
}
CONDITIONS: typing.Final = ("P0", "R0", "R1", "R2", "R4", "R8")
CONDITION_DISPLAY: typing.Final = {
    "P0": "K=0 (empty history)",
    "R0": "No-history-interface prompt control",
    "R1": "K=1",
    "R2": "K=2",
    "R4": "K=4",
    "R8": "K=8",
}
STRESS_CONDITIONS: typing.Final = (
    "r0",
    "correct_r8",
    "unrelated_predicted",
    "stale_same_source_predicted",
    "shuffled_correct_predicted",
)
STRESS_DISPLAY: typing.Final = {
    "r0": "No-history-interface prompt control",
    "correct_r8": "Ordered same-source K=8 predicted history",
    "unrelated_predicted": "Unrelated plausible predicted history",
    "stale_same_source_predicted": "Stale same-source predicted history",
    "shuffled_correct_predicted": "Shuffled same-source predicted history",
}


def _load(name: str) -> dict[str, typing.Any]:
    value = json.loads((RESULTS / name).read_bytes())
    if not isinstance(value, dict):
        message = f"{name} must contain one JSON object"
        raise TypeError(message)
    return value


def _number(value: typing.Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        message = "table metric must be numeric"
        raise TypeError(message)
    return f"{float(value):.2f}"


def _summary(value: typing.Mapping[str, typing.Any]) -> str:
    return (
        f"{_number(value['estimate'])} "
        f"[{_number(value['lower'])}, {_number(value['upper'])}]"
    )


def _tail(value: typing.Any) -> str:
    if value is None:
        return "—"
    if value == 0:
        return "0/10,000"
    return _number(value)


def render_stress(report: typing.Mapping[str, typing.Any]) -> str:
    if report.get("analysis_id") != "predicted_history_development_analysis_v1":
        message = "parent aggregate analysis identity differs"
        raise ValueError(message)
    population = report["populations"]["stress_primary_normalized_nonempty"]
    lines = [
        "## Predicted-history stress study",
        "",
        "| Model variant | Serving condition | WER | CER | "
        "Insertions/100 | Copy rate | Prior-only rate |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for target in TARGETS:
        for condition in STRESS_CONDITIONS:
            metrics = population["cells"][f"S-{condition}-{target}"]["metrics"]
            lines.append(
                "| "
                + " | ".join(
                    (
                        TARGET_DISPLAY[target],
                        STRESS_DISPLAY[condition],
                        _number(metrics["wer"]["estimate"]),
                        _number(metrics["cer"]["estimate"]),
                        _number(
                            metrics["insertions_per_100_reference_words"][
                                "estimate"
                            ]
                        ),
                        _number(metrics["context_copy_rate"]["estimate"]),
                        _number(metrics["prior_only_token_rate"]["estimate"]),
                    )
                )
                + " |"
            )
    lines.extend(
        (
            "",
            "Lexical measures are reference-proxy diagnostics, not semantic "
            "hallucination metrics.",
            "",
        )
    )
    return "\n".join(lines)


def render_extension_main(report: typing.Mapping[str, typing.Any]) -> str:
    if report.get("analysis_id") != "predicted_history_extension_analysis_v1":
        message = "extension aggregate analysis identity differs"
        raise ValueError(message)
    population = report["populations"]["main_p13_primary_4098"]
    lines = [
        "## Fixed-interface predicted-history matrix",
        "",
        f"Primary quality population: n={population['row_count']} paired rows.",
        "",
        "| Model variant | Serving condition | WER | CER | "
        "Insertions/100 | Copy rate | Prior-only rate |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for target in TARGETS:
        for condition in CONDITIONS:
            metrics = population["cells"][f"{condition}-{target}"]["metrics"]
            lines.append(
                "| "
                + " | ".join(
                    (
                        TARGET_DISPLAY[target],
                        CONDITION_DISPLAY[condition],
                        _number(metrics["wer"]["estimate"]),
                        _number(metrics["cer"]["estimate"]),
                        _number(
                            metrics["insertions_per_100_reference_words"][
                                "estimate"
                            ]
                        ),
                        _number(metrics["context_copy_rate"]["estimate"]),
                        _number(metrics["prior_only_token_rate"]["estimate"]),
                    )
                )
                + " |"
            )
    lines.extend(
        (
            "",
            "### Paired deltas",
            "",
            "| Comparison | n | Paired delta WER | 95% CI | Holm | Status |",
            "|---|---:|---:|---:|---:|---|",
        )
    )
    for target in TARGETS:
        for condition in ("R1", "R2", "R4", "R8"):
            effect = population["contrasts"][
                f"p13_{condition.lower()}_minus_p0_{target}"
            ]["metrics"]["wer"]
            lines.append(
                f"| {CONDITION_DISPLAY[condition]} − K=0 "
                f"{TARGET_DISPLAY[target]} | "
                f"{population['row_count']} | {_number(effect['estimate'])} | "
                f"[{_number(effect['lower'])}, {_number(effect['upper'])}] | "
                f"{_tail(effect.get('holm_adjusted_tail_area'))} | available |"
            )
    for target in TARGETS:
        effect = population["contrasts"][
            f"prompt_p0_minus_option_d_r0_{target}"
        ]["metrics"]["wer"]
        lines.append(
            "| K=0 − No-history-interface prompt control "
            f"{TARGET_DISPLAY[target]} | "
            f"{population['row_count']} | {_number(effect['estimate'])} | "
            f"[{_number(effect['lower'])}, {_number(effect['upper'])}] | "
            "— | available |"
        )
    lines.extend(
        (
            "",
            "K=0 renders the fixed history interface with an explicit "
            "empty-history block. The no-history-interface prompt control "
            "uses a distinct prompt and wrapper and is not a point on the "
            "K-window curve.",
            "",
        )
    )
    return "\n".join(lines)


def render_entity(report: typing.Mapping[str, typing.Any]) -> str:
    quality = report["populations"]["entity_quality_primary_498"]
    adoption = report["populations"]["entity_adoption_itt_500"]
    lines = [
        "## Reference-absent prediction lexical stress",
        "",
        f"Quality population: n={quality['row_count']} paired rows; "
        f"candidate-adoption ITT population: n={adoption['row_count']}.",
        "",
        "| Model variant | Stress condition | WER | CER | Insertions/100 | "
        "Candidate adoption |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for target in TARGETS:
        for condition in ("correct_r8", "reference_absent_entity_predicted"):
            cell_id = f"S-{condition}-{target}"
            metrics = quality["cells"][cell_id]["metrics"]
            insertion_rate = metrics["insertions_per_100_reference_words"][
                "estimate"
            ]
            adoption_rate = adoption["cells"][cell_id][
                "candidate_adoption_rate"
            ]["estimate"]
            condition_label = (
                "Ordered same-source K=8 predicted history"
                if condition == "correct_r8"
                else (
                    "Reference-absent entity-like predicted-history "
                    "intervention"
                )
            )
            lines.append(
                f"| {TARGET_DISPLAY[target]} | {condition_label} | "
                f"{_number(metrics['wer']['estimate'])} | "
                f"{_number(metrics['cer']['estimate'])} | "
                f"{_number(insertion_rate)} | "
                f"{_number(adoption_rate)} |"
            )
    lines.extend(
        (
            "",
            "### Paired deltas",
            "",
            "| Model variant | Quality n | Paired delta WER | 95% CI | "
            "Holm | Adoption n | Candidate-adoption delta | 95% CI | "
            "Holm | Status |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        )
    )
    for target in TARGETS:
        wer = quality["contrasts"][
            f"entity_intervention_minus_correct_{target}"
        ]["metrics"]["wer"]
        adopted = adoption["contrasts"][
            f"entity_adoption_intervention_minus_correct_{target}"
        ]["candidate_adoption_rate"]
        lines.append(
            f"| {TARGET_DISPLAY[target]} | {quality['row_count']} | "
            f"{_number(wer['estimate'])} | "
            f"[{_number(wer['lower'])}, {_number(wer['upper'])}] | "
            f"{_tail(wer.get('holm_adjusted_tail_area'))} | "
            f"{adoption['row_count']} | {_number(adopted['estimate'])} | "
            f"[{_number(adopted['lower'])}, {_number(adopted['upper'])}] | "
            f"{_tail(adopted.get('holm_adjusted_tail_area'))} | available |"
        )
    lines.extend(
        (
            "",
            "Candidate adoption is an intent-to-treat lexical diagnostic. "
            "It does not measure acoustic support or semantic hallucination.",
            "",
        )
    )
    return "\n".join(lines)


def render_breakdown(report: typing.Mapping[str, typing.Any]) -> str:
    if (
        report.get("analysis_id")
        != "predicted_history_postoutcome_breakdown_v1"
    ):
        message = "breakdown aggregate analysis identity differs"
        raise ValueError(message)
    family_ids = sorted(
        str(item["stratum_id"])
        for item in report["strata"]
        if item["dimension"] == "source_family"
    )
    lines = [
        "# Predicted-history descriptive breakdown",
        "",
        "**Post-outcome exploratory analysis.** Intervals are descriptive "
        "95% source-cluster-then-row percentile intervals; subgroup patterns "
        "are not confirmatory or causal.",
        "",
        "| Dimension | Stratum | Rows | Clusters | Model variant | "
        "No-history-interface control WER | K=1 WER | K=8 WER | "
        "K=8 − K=1 (pp) |",
        "|---|---:|---:|---:|---|---:|---:|---:|---:|",
    ]
    for stratum in report["strata"]:
        label = str(stratum["stratum_id"])
        if stratum["dimension"] == "source_family":
            label = f"Family {chr(65 + family_ids.index(label))}"
        for target in TARGETS:
            record = stratum["targets"][target]
            lines.append(
                f"| {stratum['dimension']} | {label} | "
                f"{stratum['row_count']} | "
                f"{stratum['source_cluster_count']} | "
                f"{TARGET_DISPLAY[target]} | "
                f"{_summary(record['cells']['R0']['wer'])} | "
                f"{_summary(record['cells']['R1']['wer'])} | "
                f"{_summary(record['cells']['R8']['wer'])} | "
                f"{_summary(record['effects']['R8_minus_R1']['wer'])} |"
            )
    lines.extend(
        (
            "",
            "K=1 and K=8 use the fixed history-interface prompt; the "
            "no-history-interface control uses the registered context-free "
            "prompt, so control-to-nonzero differences remain "
            "prompt-plus-history bundles. Companion JSON/CSV include "
            "automatic context-copy and prior-only token reference proxies "
            "where their history-conditioned denominators are defined. "
            "Context-copy is the percentage of successful rows with nonempty "
            "safety-tokenized realized history whose prediction has a "
            "contiguous span of at least three tokens found within one prior "
            "turn but absent from the current reference; rows without realized "
            "history contribute to neither numerator nor denominator. "
            "Prior-only token rate uses all safety-token occurrences in "
            "successful predictions as its denominator. This automatic proxy "
            "analysis does not measure semantic hallucination or safety.",
            "",
            "Reproduction requires the ignored authenticated input binding "
            "and an owner-only exact 32-byte HMAC salt supplied through "
            "`$OWNER_ONLY_HMAC_SALT_FILE`; neither private input is published.",
            "The result JSON records a locked-environment command using fresh "
            "`reproductions/` destinations plus byte-comparison commands; "
            "canonical create-only destinations are never overwritten.",
            "",
        )
    )
    return "\n".join(lines)


def _write_create_only(path: pathlib.Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="") as handle:
        handle.write(content)


def main(argv: typing.Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    args = parser.parse_args(argv)
    parent = _load("predicted_history_analysis.json")
    extension = _load("predicted_history_extension_analysis.json")
    breakdown = _load("predicted_history_breakdown.json")
    outputs = {
        "predicted_history_stress.md": render_stress(parent),
        "predicted_history_extension_main.md": render_extension_main(extension),
        "predicted_history_reference_absent_entity.md": render_entity(
            extension
        ),
        "predicted_history_breakdown.md": render_breakdown(breakdown),
    }
    for name, content in outputs.items():
        _write_create_only(args.output_dir / name, content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
