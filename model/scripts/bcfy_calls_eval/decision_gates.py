"""Decision-gate analysis for the framing-context A/B (pure, testable).

Per model (gemini, chirp) it compares the moderate arm against baseline on the
*framed* segments (segments whose parent call has a usable framing record —
unframed segments get a byte-identical prompt in both arms, so their delta is
structurally 0 and would only dilute the estimate).

Sign convention (fixed once, to avoid the spec's ambiguous wording):

    improvement_i = WER(baseline)_i - WER(moderate)_i      # positive = framing better

All gates are expressed in `improvement`. Deltas are paired per segment and
summarized with a 10k paired-bootstrap CI (seed 42).

Gates (per model, independent):
- hard_short_clip : lower 95% CI of mean improvement >= 0 on BOTH the 1-2 and
  3-5 GT-word buckets — 95% confident framing is non-worse where short clips
  dominate. (Binding; catches the prior ~0.8pt short-clip regression.)
- net_signal      : lower 95% CI of mean aggregate improvement > 0 — 95%
  confident framing is strictly better overall.
- per_group       : flag any system whose upper 95% CI of improvement < 0;
  BLOCK if any system is 95%-confident worse by > 2 WER points.
- over_insertion  : mean over-insertion (pred tokens absent from GT) on short
  clips, moderate <= baseline.

`analyze` returns a JSON-able report; `render_markdown` formats it.
"""

from __future__ import annotations

import random
from collections import defaultdict
from typing import Any

BUCKETS = (
    ("1-2", 1, 2),
    ("3-5", 3, 5),
    ("6-10", 6, 10),
    ("11-20", 11, 20),
    ("21+", 21, 10**9),
)
SHORT_BUCKETS = ("1-2", "3-5")
BOOTSTRAP_N = 10_000
BOOTSTRAP_SEED = 42
MATERIALLY_WORSE_PTS = 2.0  # per-group block threshold


def bucket_of(gt_words: int) -> str:
    for label, lo, hi in BUCKETS:
        if lo <= gt_words <= hi:
            return label
    return "21+"


def paired_bootstrap_ci(
    deltas: list[float], n: int = BOOTSTRAP_N, seed: int = BOOTSTRAP_SEED
) -> tuple[float, float]:
    """95% CI for the mean of `deltas` via paired bootstrap resampling."""
    if not deltas:
        return (0.0, 0.0)
    if len(deltas) == 1:
        return (deltas[0], deltas[0])
    rng = random.Random(seed)
    k = len(deltas)
    means = []
    for _ in range(n):
        s = 0.0
        for _ in range(k):
            s += deltas[rng.randrange(k)]
        means.append(s / k)
    means.sort()
    lo = means[int(0.025 * n)]
    hi = means[int(0.975 * n)]
    return (round(lo, 3), round(hi, 3))


def _mean(xs: list[float]) -> float:
    return round(sum(xs) / len(xs), 3) if xs else 0.0


def _model_improvements(
    scored: list[dict[str, Any]], model: str, framed_only: bool
) -> list[dict[str, Any]]:
    """Per-segment improvement rows for one model (baseline WER - moderate WER)."""
    b, m = f"wer_{model}_baseline", f"wer_{model}_moderate"
    ib, im = f"ins_{model}_baseline", f"ins_{model}_moderate"
    rows = []
    for e in scored:
        if framed_only and not e.get("framed"):
            continue
        if b not in e or m not in e:
            continue
        rows.append({
            "example_id": e["example_id"],
            "system": e.get("system", "unknown"),
            "gt_words": e["gt_words"],
            "bucket": bucket_of(e["gt_words"]),
            "improvement": e[b] - e[m],
            "ins_baseline": e.get(ib, 0),
            "ins_moderate": e.get(im, 0),
            "wer_baseline": e[b],
            "wer_moderate": e[m],
        })
    return rows


def _bucket_table(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    by_b: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_b[r["bucket"]].append(r)
    for label, _, _ in BUCKETS:
        sub = by_b.get(label, [])
        deltas = [r["improvement"] for r in sub]
        lo, hi = paired_bootstrap_ci(deltas)
        out[label] = {
            "n": len(sub),
            "mean_improvement": _mean(deltas),
            "ci_lo": lo,
            "ci_hi": hi,
            "wer_baseline": _mean([r["wer_baseline"] for r in sub]),
            "wer_moderate": _mean([r["wer_moderate"] for r in sub]),
        }
    return out


def _per_group(rows: list[dict[str, Any]], min_n: int = 10) -> list[dict[str, Any]]:
    by_g: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_g[r["system"]].append(r)
    out = []
    for g, sub in by_g.items():
        deltas = [r["improvement"] for r in sub]
        lo, hi = paired_bootstrap_ci(deltas)
        out.append({
            "system": g,
            "n": len(sub),
            "mean_improvement": _mean(deltas),
            "ci_lo": lo,
            "ci_hi": hi,
            "powered": len(sub) >= min_n,
        })
    out.sort(key=lambda x: x["mean_improvement"])
    return out


def _analyze_model(scored: list[dict[str, Any]], model: str) -> dict[str, Any]:
    rows = _model_improvements(scored, model, framed_only=True)
    n_framed = len(rows)
    buckets = _bucket_table(rows)
    all_deltas = [r["improvement"] for r in rows]
    agg_lo, agg_hi = paired_bootstrap_ci(all_deltas)
    groups = _per_group(rows)

    # Gate 1: hard short-clip — lower CI >= 0 on both short buckets (only if present).
    short_eval = {b: buckets[b] for b in SHORT_BUCKETS if buckets[b]["n"] > 0}
    hard_pass = bool(short_eval) and all(v["ci_lo"] >= 0 for v in short_eval.values())

    # Gate 2: net signal — lower CI of aggregate improvement > 0.
    net_pass = n_framed > 0 and agg_lo > 0

    # Gate 3: per-group — flag worse groups; block if any powered group worse by >2 pts.
    flagged = [g for g in groups if g["ci_hi"] < 0]
    blocked = [g for g in groups if g["powered"] and g["ci_hi"] < -MATERIALLY_WORSE_PTS]
    per_group_pass = not blocked

    # Gate 4: over-insertion on short clips — moderate <= baseline (mean).
    short_rows = [r for r in rows if r["bucket"] in SHORT_BUCKETS]
    ins_base = _mean([r["ins_baseline"] for r in short_rows])
    ins_mod = _mean([r["ins_moderate"] for r in short_rows])
    overins_pass = ins_mod <= ins_base + 1e-9

    gates = {
        "hard_short_clip": hard_pass,
        "net_signal": net_pass,
        "per_group": per_group_pass,
        "over_insertion": overins_pass,
    }
    # Outcome: ship moderate only if hard + net + per_group hold (over-insertion advisory).
    ship = hard_pass and net_pass and per_group_pass
    return {
        "model": model,
        "n_framed_segments": n_framed,
        "aggregate": {
            "mean_improvement": _mean(all_deltas),
            "ci_lo": agg_lo, "ci_hi": agg_hi,
        },
        "buckets": buckets,
        "short_clip_overinsertion": {"baseline": ins_base, "moderate": ins_mod},
        "per_group": groups,
        "flagged_groups": [g["system"] for g in flagged],
        "blocked_groups": [g["system"] for g in blocked],
        "gates": gates,
        "ship_moderate": ship,
    }


def analyze(
    scored: list[dict[str, Any]],
    arms: list[str],
    framing_variants: list[str],
) -> dict[str, Any]:
    """Top-level report across all models present in `arms`."""
    models = sorted({a.rsplit("_", 1)[0] for a in arms})
    n_total = len(scored)
    n_framed = sum(1 for e in scored if e.get("framed"))
    return {
        "experiment": "framing_context_05_2026",
        "framing_variants": framing_variants,
        "total_scored_segments": n_total,
        "framed_segments": n_framed,
        "framed_pct": round(100 * n_framed / max(1, n_total), 1),
        "models": {m: _analyze_model(scored, m) for m in models},
    }


def render_markdown(report: dict[str, Any]) -> str:
    L = [f"# Framing-context A/B — decision report ({report['experiment']})", ""]
    L.append(
        f"Scored **{report['total_scored_segments']}** segments; "
        f"**{report['framed_segments']}** ({report['framed_pct']}%) framed "
        f"(analysis below restricted to framed segments)."
    )
    L.append("")
    for model, r in report["models"].items():
        agg = r["aggregate"]
        L.append(f"## {model}  —  {'✅ SHIP moderate' if r['ship_moderate'] else '❌ do NOT ship'}")
        L.append("")
        L.append(f"Framed segments: {r['n_framed_segments']}. "
                 f"Sign: improvement = baseline_WER − moderate_WER (positive = framing better).")
        L.append("")
        L.append("**Gates:**")
        for g, ok in r["gates"].items():
            L.append(f"- {'✅' if ok else '❌'} `{g}`")
        L.append("")
        L.append(f"**Aggregate improvement:** {agg['mean_improvement']:+.2f} WER pts "
                 f"(95% CI [{agg['ci_lo']:+.2f}, {agg['ci_hi']:+.2f}])")
        L.append("")
        L.append("**By GT-word bucket** (improvement, +=better):")
        L.append("")
        L.append("| Bucket | n | base WER | mod WER | improvement | 95% CI |")
        L.append("|---|--:|--:|--:|--:|--:|")
        for b, _, _ in BUCKETS:
            v = r["buckets"][b]
            star = " ⟵ hard gate" if b in SHORT_BUCKETS else ""
            L.append(f"| {b}{star} | {v['n']} | {v['wer_baseline']:.1f} | "
                     f"{v['wer_moderate']:.1f} | {v['mean_improvement']:+.2f} | "
                     f"[{v['ci_lo']:+.2f}, {v['ci_hi']:+.2f}] |")
        L.append("")
        oi = r["short_clip_overinsertion"]
        L.append(f"**Short-clip over-insertion:** baseline {oi['baseline']:.2f} "
                 f"vs moderate {oi['moderate']:.2f} tokens/seg")
        if r["flagged_groups"]:
            L.append("")
            L.append(f"**Flagged systems (CI worse):** {', '.join(r['flagged_groups'][:10])}")
        if r["blocked_groups"]:
            L.append(f"**BLOCKING systems (>2pt worse):** {', '.join(r['blocked_groups'])}")
        L.append("")
    return "\n".join(L)
