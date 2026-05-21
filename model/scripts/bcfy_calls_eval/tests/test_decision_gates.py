"""Unit tests for decision_gates — bucketing, bootstrap CI, and gate logic."""

from __future__ import annotations

from bcfy_calls_eval import decision_gates as dg


# --- bucketing ------------------------------------------------------------


def test_bucket_of():
    assert dg.bucket_of(1) == "1-2"
    assert dg.bucket_of(2) == "1-2"
    assert dg.bucket_of(3) == "3-5"
    assert dg.bucket_of(5) == "3-5"
    assert dg.bucket_of(10) == "6-10"
    assert dg.bucket_of(20) == "11-20"
    assert dg.bucket_of(21) == "21+"
    assert dg.bucket_of(999) == "21+"


# --- bootstrap CI ---------------------------------------------------------


def test_bootstrap_ci_deterministic():
    deltas = [1.0, -2.0, 3.0, 0.5, -1.5, 2.0]
    a = dg.paired_bootstrap_ci(deltas)
    b = dg.paired_bootstrap_ci(deltas)
    assert a == b  # fixed seed -> reproducible


def test_bootstrap_ci_positive_signal():
    # Strongly positive deltas -> CI entirely above 0.
    deltas = [5.0] * 50
    lo, hi = dg.paired_bootstrap_ci(deltas)
    assert lo == 5.0 and hi == 5.0


def test_bootstrap_ci_empty():
    assert dg.paired_bootstrap_ci([]) == (0.0, 0.0)


# --- gate logic via analyze ----------------------------------------------


def _seg(eid, gt_words, wer_base, wer_mod, system="SysA", framed=True, ins_b=0, ins_m=0):
    return {
        "example_id": eid,
        "gt_words": gt_words,
        "framed": framed,
        "system": system,
        "wer_gemini_baseline": wer_base,
        "wer_gemini_moderate": wer_mod,
        "ins_gemini_baseline": ins_b,
        "ins_gemini_moderate": ins_m,
    }


def test_analyze_ships_when_framing_clearly_better():
    # Framing improves WER by 10 pts everywhere, across short + long buckets.
    scored = []
    for i in range(40):
        gw = 1 + (i % 5)  # spread across 1-2 and 3-5 buckets
        scored.append(_seg(f"e{i}-1", gw, wer_base=50.0, wer_mod=40.0))
    rep = dg.analyze(scored, arms=["gemini_baseline", "gemini_moderate"],
                     framing_variants=["baseline", "moderate"])
    g = rep["models"]["gemini"]
    assert g["gates"]["hard_short_clip"] is True
    assert g["gates"]["net_signal"] is True
    assert g["ship_moderate"] is True


def test_analyze_blocks_when_framing_worse_on_short():
    # Framing makes short clips worse (the prior failure mode).
    scored = []
    for i in range(40):
        gw = 1 + (i % 2)  # all short (1-2)
        scored.append(_seg(f"e{i}-1", gw, wer_base=40.0, wer_mod=50.0))
    rep = dg.analyze(scored, arms=["gemini_baseline", "gemini_moderate"],
                     framing_variants=["baseline", "moderate"])
    g = rep["models"]["gemini"]
    assert g["gates"]["hard_short_clip"] is False
    assert g["ship_moderate"] is False


def test_analyze_restricts_to_framed_segments():
    # Unframed segments must be excluded from the improvement estimate.
    scored = [_seg("f-1", 2, 50.0, 30.0, framed=True)]
    scored += [_seg(f"u{i}-1", 2, 50.0, 50.0, framed=False) for i in range(20)]
    rep = dg.analyze(scored, arms=["gemini_baseline", "gemini_moderate"],
                     framing_variants=["baseline", "moderate"])
    assert rep["models"]["gemini"]["n_framed_segments"] == 1


def test_analyze_over_insertion_gate():
    # Moderate over-inserts more on short clips -> over_insertion gate fails.
    scored = [
        _seg(f"e{i}-1", 2, 40.0, 38.0, ins_b=0, ins_m=3) for i in range(30)
    ]
    rep = dg.analyze(scored, arms=["gemini_baseline", "gemini_moderate"],
                     framing_variants=["baseline", "moderate"])
    assert rep["models"]["gemini"]["gates"]["over_insertion"] is False


def test_render_markdown_smoke():
    scored = [_seg(f"e{i}-1", 2, 50.0, 40.0) for i in range(10)]
    rep = dg.analyze(scored, arms=["gemini_baseline", "gemini_moderate"],
                     framing_variants=["baseline", "moderate"])
    md = dg.render_markdown(rep)
    assert "decision report" in md
    assert "gemini" in md
