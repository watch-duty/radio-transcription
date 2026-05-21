#!/usr/bin/env python3
"""Render a self-contained HTML progress report from a decision_report.json.

Dependency-light (stdlib only): the per-bucket baseline-vs-moderate WER chart
is inline SVG, so this runs in any venv. Reused verbatim for smoke and full
results.

    python research/make_report.py [decision_report.json] [out.html]

Defaults: results/framing_context_05_2026/decision_report.json ->
research/to_human/progress_<YYYY-MM-DD>.html
"""

from __future__ import annotations

import datetime as _dt
import html
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
EXP_DIR = SCRIPT_DIR.parent / "results" / "framing_context_05_2026"
BUCKETS = ["1-2", "3-5", "6-10", "11-20", "21+"]


def _bar_chart(buckets: dict) -> str:
    """Grouped bars: baseline (grey) vs moderate (blue) capped WER per bucket."""
    w, h, pad, gw = 560, 220, 34, 96
    maxv = max([1.0] + [max(buckets[b]["wer_baseline"], buckets[b]["wer_moderate"])
                        for b in BUCKETS if buckets[b]["n"]])
    svg = [f'<svg width="{w}" height="{h}" font-family="sans-serif" font-size="11">']
    y0 = h - pad
    svg.append(f'<line x1="{pad}" y1="{y0}" x2="{w}" y2="{y0}" stroke="#999"/>')
    for i, b in enumerate(BUCKETS):
        v = buckets[b]
        if not v["n"]:
            continue
        x = pad + 8 + i * gw
        for j, (key, color) in enumerate((("wer_baseline", "#9aa5b1"),
                                          ("wer_moderate", "#2b6cb0"))):
            bh = (v[key] / maxv) * (h - 2 * pad)
            bx = x + j * 26
            svg.append(f'<rect x="{bx}" y="{y0 - bh:.1f}" width="24" '
                       f'height="{bh:.1f}" fill="{color}"/>')
            svg.append(f'<text x="{bx + 12}" y="{y0 - bh - 3:.1f}" '
                       f'text-anchor="middle">{v[key]:.0f}</text>')
        svg.append(f'<text x="{x + 25}" y="{y0 + 14}" text-anchor="middle">'
                   f'{b} (n={v["n"]})</text>')
    svg.append(f'<rect x="{pad}" y="6" width="12" height="12" fill="#9aa5b1"/>'
               f'<text x="{pad + 16}" y="16">baseline</text>'
               f'<rect x="{pad + 80}" y="6" width="12" height="12" fill="#2b6cb0"/>'
               f'<text x="{pad + 96}" y="16">moderate (framed)</text>')
    svg.append("</svg>")
    return "".join(svg)


def _model_section(model: str, r: dict) -> str:
    verdict = "✅ SHIP moderate" if r["ship_moderate"] else "❌ do NOT ship"
    gates = " ".join(f'<span class="g {"ok" if ok else "no"}">{g}</span>'
                     for g, ok in r["gates"].items())
    agg = r["aggregate"]
    rw = r.get("raw_wer", {})
    hr = r.get("hallucination_rate_pct", {})
    oi = r["short_clip_overinsertion"]
    rows = "".join(
        f"<tr><td>{b}{' ⟵ gate' if b in ('1-2','3-5') else ''}</td>"
        f"<td>{r['buckets'][b]['n']}</td>"
        f"<td>{r['buckets'][b]['wer_baseline']:.1f}</td>"
        f"<td>{r['buckets'][b]['wer_moderate']:.1f}</td>"
        f"<td>{r['buckets'][b]['mean_improvement']:+.2f}</td>"
        f"<td>[{r['buckets'][b]['ci_lo']:+.2f}, {r['buckets'][b]['ci_hi']:+.2f}]</td></tr>"
        for b in BUCKETS
    )
    raw_line = (f"Raw WER {rw['baseline']:.1f}→{rw['moderate']:.1f} · "
                f"hallucination {hr['baseline']:.1f}%→{hr['moderate']:.1f}% · "
                f"short-clip over-insertion {oi['baseline']:.2f}→{oi['moderate']:.2f}"
                if rw else "")
    return f"""
    <h2>{html.escape(model)} &mdash; {verdict}</h2>
    <p class="gates">{gates}</p>
    <p>Framed segments: <b>{r['n_framed_segments']}</b>. Metric: capped WER
       (per-segment min(wer,100)). Aggregate improvement
       <b>{agg['mean_improvement']:+.2f}</b> pts (95% CI [{agg['ci_lo']:+.2f},
       {agg['ci_hi']:+.2f}]).</p>
    <p class="mech">{raw_line}</p>
    {_bar_chart(r['buckets'])}
    <table><tr><th>bucket</th><th>n</th><th>base</th><th>mod</th>
      <th>improve</th><th>95% CI</th></tr>{rows}</table>
    """


def main() -> int:
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else EXP_DIR / "decision_report.json"
    rep = json.loads(src.read_text())
    today = _dt.date.today().isoformat()
    out = (Path(sys.argv[2]) if len(sys.argv) > 2
           else SCRIPT_DIR / "to_human" / f"progress_{today}.html")
    out.parent.mkdir(parents=True, exist_ok=True)

    scope = (f"Scored {rep['total_scored_segments']} segments; "
             f"{rep['framed_segments']} ({rep['framed_pct']}%) framed "
             f"(analysis restricted to framed).")
    body = "".join(_model_section(m, r) for m, r in rep["models"].items())
    doc = f"""<!doctype html><html><head><meta charset="utf-8">
<title>bcfy_calls framing A/B — {today}</title><style>
body{{font-family:sans-serif;max-width:760px;margin:2rem auto;color:#1a202c;line-height:1.5}}
h1{{font-size:1.4rem}} h2{{margin-top:2rem;border-top:1px solid #e2e8f0;padding-top:1rem}}
table{{border-collapse:collapse;margin:1rem 0;font-size:13px}}
td,th{{border:1px solid #e2e8f0;padding:3px 8px;text-align:right}} td:first-child,th:first-child{{text-align:left}}
.gates .g{{padding:2px 8px;border-radius:4px;margin-right:6px;font-size:12px}}
.g.ok{{background:#c6f6d5}} .g.no{{background:#fed7d7}}
.mech{{color:#4a5568;font-size:13px}} .q{{background:#ebf8ff;padding:1rem;border-radius:6px}}
</style></head><body>
<h1>Does metadata framing improve bcfy_calls transcription?</h1>
<p class="q"><b>Question:</b> does appending per-call descriptive metadata
(system / agency / service / location, as prose) to the production prompt
reduce WER for Chirp V3 and Gemini 3.1 Flash Lite, without regressing the
short clips that dominate traffic?</p>
<p>{scope}</p>
{body}
<p style="color:#718096;font-size:12px;margin-top:2rem">Sign: improvement =
baseline − moderate (positive = framing better). Generated {today}.</p>
</body></html>"""
    out.write_text(doc)
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
