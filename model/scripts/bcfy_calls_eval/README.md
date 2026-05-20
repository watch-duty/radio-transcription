# bcfy_calls_eval — Framing-context A/B on the Broadcastify Calls eval set

Headless Python pipeline that transcribes the Broadcastify Calls eval set
(`gs://wd-transcription-data/segmented_audio/broadcastify/calls/eval/audio_raw/batch_manifest.jsonl`)
with **Chirp V3** + **Gemini 3.1 Flash Lite**, **three prompt variants** each
(baseline / Moderate framing / Aggressive framing), and scores with the same
jiwer + NeMo + dispatch-quirks normalization as `echo_eval/run_eval.py`.

Spec: `/home/shuojing/.claude/plans/glistening-swinging-bachman.md`.
Plan: `radio-transcription/docs/superpowers/plans/2026-05-20-broadcastify-framing-context.md`.

## Setup

```bash
cd radio-transcription/model/scripts/bcfy_calls_eval
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

`nemo_text_processing` pulls `pynini` (needs OpenFst). If the build fails,
install OpenFst headers first.

**GCP auth:** ADC must be configured for project `automatic-hawk-481415-m9`.
**Broadcastify auth:** export `BROADCASTIFY_APP_ID`, `BROADCASTIFY_API_KEY_ID`,
`BROADCASTIFY_API_TOKEN` (same env vars as `bcfy_api.py`).

## Run

```bash
# One-time per eval-manifest version: build the per-talkgroup context records.
python build_context_records.py

# Smoke (~$2–5, minutes): 50 segments × 6 arms.
python run_eval.py all --limit 50

# Full (~$50–150, ~1–2 hr wall): all segments × 6 arms.
python run_eval.py all
```

Stages can also be run independently:

```bash
python run_eval.py transcribe   # submit all 6 Vertex+Chirp jobs in parallel
python run_eval.py merge        # merge all 6 prediction sets into one manifest
python run_eval.py score        # jiwer + NeMo + decision gates → wer_summary.md
```

## Decision gates

Reported per (model, variant) in `wer_summary.md`. **Hard short-clip
non-regression** is the binding constraint — see the spec.

## Results

After `score`, the headline appears at the top of
`results/<EXPERIMENT_NAME>/wer_summary.md`. Per-prompt success-set WER,
length-bucketed WER (1-2 / 3-5 / 6-10 / 11-20 / 21+ GT words), per-talkgroup
WER, paired-bootstrap CIs (10k, seed 42), hallucination rate, and the four
decision gates per (model, variant) are all reported.

Re-running with the same `EXPERIMENT_NAME` overwrites GCS artifacts. To
preserve prior results, edit `EXPERIMENT_NAME` in `run_eval.py`.
