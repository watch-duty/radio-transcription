# bcfy_calls_eval — Framing-context A/B on the Broadcastify Calls eval set

Headless Python pipeline that transcribes the Broadcastify Calls eval set
(`gs://wd-transcription-data/segmented_audio/broadcastify/calls/eval/audio_raw/batch_manifest.jsonl`)
with **Chirp V3** + **Gemini 3.1 Flash Lite**, in **two framing variants** each
(**baseline** = production prompt, **moderate** = production prompt + per-call
descriptive framing) — **4 arms** — and scores with the same jiwer + NeMo +
dispatch-quirks normalization as `echo_eval/run_eval.py`, plus length-bucketed
WER, paired-bootstrap CIs, and the spec's decision gates.

> **Aggressive** framing was dropped: it differed from Moderate on only ~2% of
> clips (the scanner tags rarely carry a distinct alpha tag *and* descriptor).

## Where the framing context comes from

**The eval clips' own WAV metadata**, not the Broadcastify API. The filename
integer is a talkgroup decimal (`tgDec`), not a system `sid` — the sid was
dropped when the dataset was built, so the API can't recover per-call context
(it mis-resolves; see `FINDINGS.md`). Each clip's scanner-software tags
(ProScan / SDRTrunk) carry the system, site, and talkgroup identity directly.
`build_context_records.py` parses them (`wav_context.py`) into the records the
framing renderer consumes. Coverage: **202/277 (72.9%)** of calls frame; the
rest fall back to baseline in every arm (neutral to the A/B).

## Setup

```bash
cd radio-transcription/model/scripts/bcfy_calls_eval
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`nemo_text_processing` pulls `pynini` (needs OpenFst). If the build fails,
install OpenFst headers first. `ffprobe` (ffmpeg) and `gsutil` must be on PATH.

**GCP auth:** ADC configured for project `automatic-hawk-481415-m9`. No
Broadcastify credentials are needed (the API path was retired).

## Run

```bash
# One-time per eval-manifest version: build per-call context records from
# the clips' WAV metadata (downloads WAVs into .wav_cache/, runs ffprobe).
python -m bcfy_calls_eval.build_context_records

# Smoke (~$1.5–3, minutes): 50 segments × 4 arms.
python -m bcfy_calls_eval.run_eval all --limit 50

# Full (~$35–100, ~30–60 min wall): all segments × 4 arms.
python -m bcfy_calls_eval.run_eval all
```

Stages run independently; `--arms` restricts to one backend:

```bash
python -m bcfy_calls_eval.run_eval transcribe --arms gemini   # Vertex batch
python -m bcfy_calls_eval.run_eval transcribe --arms chirp    # STT v2 sync recognize
python -m bcfy_calls_eval.run_eval merge
python -m bcfy_calls_eval.run_eval score
```

Both `transcribe` stages are idempotent: re-running skips URIs already present
in each arm's `predictions.jsonl`, so adding an arm doesn't re-spend on others.

## Decision gates (per model, in `decision_report.md`)

Sign: `improvement = baseline_WER − moderate_WER` (positive = framing better).
Analysis is restricted to the **framed** segments.

- **hard_short_clip** (binding): lower 95% CI of mean improvement ≥ 0 on *both*
  the 1-2 and 3-5 GT-word buckets — framing must be statistically non-worse
  where short clips dominate. Catches the prior ~0.8pt short-clip regression.
- **net_signal**: lower 95% CI of aggregate improvement > 0 (strictly better).
- **per_group**: flag any system whose improvement CI is < 0; BLOCK if any
  powered system is >2 WER pts worse.
- **over_insertion**: short-clip over-insertion, moderate ≤ baseline.

`ship_moderate` = hard_short_clip ∧ net_signal ∧ per_group (over_insertion
advisory). Outcomes can differ between Chirp and Gemini — that steers which
model (if any) gets a productionization spec.

## Results

`results/<EXPERIMENT_NAME>/`:
- `context_records.json` / `context_report.json` — per-call framing + coverage
- `merged.jsonl` — manifest with all 4 arms' predictions
- `eval_per_sample.jsonl` — per-segment GT, preds, WER, over-insertion
- `decision_report.{md,json}` — bucketed WER, CIs, gates, per-model verdict

Re-running with the same `EXPERIMENT_NAME` overwrites GCS artifacts; bump it in
`run_eval.py` to preserve prior results.
