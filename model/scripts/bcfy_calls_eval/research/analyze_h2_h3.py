#!/usr/bin/env python3
"""H2 (effect vs metadata richness) + service breakdown + H3 (effect vs length).

Pure re-analysis of the EXISTING full-run predictions — no new inference.
Joins eval_per_sample.jsonl (per-segment capped WER per arm) with
context_records.json (per-call sName / county / state / tagDescr) and slices
the framing improvement (baseline − moderate, capped WER) by:

  H2  metadata richness tier: agency-only → +system → +geo(county&state)
      prediction: richer metadata → larger improvement.
  SVC service class (fire / law / ems / paging) — WatchDuty cares about fire.
  H3  GT-word length bucket — prediction was "helps long more"; the data tests it.

Writes research/to_human/h2_h3_analysis.md.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

from bcfy_calls_eval.decision_gates import bucket_of, paired_bootstrap_ci

SCRIPT_DIR = Path(__file__).resolve().parent
EXP = SCRIPT_DIR.parent / "results" / "framing_context_05_2026"
MODELS = ("gemini", "chirp")


def _service_class(tag: str) -> str:
    t = (tag or "").lower()
    if "fire" in t and "ems" in t:
        return "fire/EMS"
    if "fire" in t:
        return "fire"
    if "medical" in t or "ems" in t:
        return "EMS"
    if "law" in t:
        return "law enforcement"
    if "paging" in t:
        return "paging"
    return "other"


def _richness(rec: dict) -> str:
    has_sys = bool(rec.get("sName"))
    has_geo = bool(rec.get("county") and rec.get("state"))
    if has_geo:
        return "3 +geo (county&state)"
    if has_sys:
        return "2 +system"
    return "1 agency+service only"


def _ci_row(deltas: list[float]) -> tuple[int, float, float, float]:
    lo, hi = paired_bootstrap_ci(deltas)
    mean = round(sum(deltas) / len(deltas), 2) if deltas else 0.0
    return len(deltas), mean, lo, hi


def _slice_table(rows, keyfn, order=None) -> list[tuple]:
    groups = defaultdict(list)
    for r in rows:
        groups[keyfn(r)].append(r["improvement"])
    keys = order or sorted(groups)
    out = []
    for k in keys:
        if k in groups:
            n, mean, lo, hi = _ci_row(groups[k])
            out.append((k, n, mean, lo, hi))
    return out


def _fmt(table) -> str:
    lines = ["| group | n | improvement | 95% CI |", "|---|--:|--:|--:|"]
    for k, n, mean, lo, hi in table:
        lines.append(f"| {k} | {n} | {mean:+.2f} | [{lo:+.2f}, {hi:+.2f}] |")
    return "\n".join(lines)


def main() -> int:
    recs = json.loads((EXP / "context_records.json").read_text())
    scored = [json.loads(l) for l in (EXP / "eval_per_sample.jsonl").read_text().splitlines() if l.strip()]

    out = ["# H2 / H3 — framing-effect slices (re-analysis, no new inference)", "",
           "Improvement = baseline − moderate (capped WER, positive = framing better). "
           "Framed segments only.", ""]

    for model in MODELS:
        cb, cm = f"wer_capped_{model}_baseline", f"wer_capped_{model}_moderate"
        rows = []
        for e in scored:
            if not e.get("framed") or cb not in e or cm not in e:
                continue
            rec = recs.get(e["example_id"], {})
            rows.append({
                "improvement": e[cb] - e[cm],
                "richness": _richness(rec),
                "service": _service_class(rec.get("tagDescr")),
                "bucket": bucket_of(e["gt_words"]),
            })
        out.append(f"## {model}  (n={len(rows)} framed)")
        out.append("")
        out.append("**H2 — by metadata richness:**")
        out.append(_fmt(_slice_table(rows, lambda r: r["richness"],
                                     order=["1 agency+service only", "2 +system", "3 +geo (county&state)"])))
        out.append("")
        out.append("**By service class** (WatchDuty cares about fire):")
        out.append(_fmt(_slice_table(rows, lambda r: r["service"])))
        out.append("")
        out.append("**H3 — by GT-word length bucket:**")
        out.append(_fmt(_slice_table(rows, lambda r: r["bucket"],
                                     order=["1-2", "3-5", "6-10", "11-20", "21+"])))
        out.append("")

    dst = SCRIPT_DIR / "to_human" / "h2_h3_analysis.md"
    dst.write_text("\n".join(out))
    print(f"wrote {dst}\n")
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
