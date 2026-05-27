# Watch Duty Radio Transcription Gemini SFT Pipeline

A re-runnable pipeline for Gemini supervised fine-tuning (Gemini SFT) of
Watch Duty's emergency-radio transcription model on Vertex AI.

## Subcommands

```
python pipeline.py build   Build Gemini SFT JSONL from registered datasets
python pipeline.py tune    Submit Vertex AI Gemini SFT tuning job (--confirm required; ~$55-175/run)
python pipeline.py eval    Batch-infer and score a Gemini model on the held-out manifest
python pipeline.py all     build -> tune -> eval in one Gemini SFT invocation
```

## Runtime

Default local runtime is the repo's lightweight ASR experiment Docker service.
It mounts the repo at `/workspace` and bootstraps the local `common` package as
`/workspace/model[scoring,vertex]` on container startup, so the Gemini SFT CLI can
run without a separate local pip install.

From the repo root:

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'cd /workspace/model/scripts/sft && python pipeline.py --help'
```

Use `notebooks-cpu` for Gemini SFT CLI work. The paid tune/eval jobs run remotely on
Vertex AI, so no local GPU is required. The `notebooks` service remains available for
GPU-backed notebook workflows that need it.

## Local Installation Fallback

From this directory (`model/scripts/sft/`):

```bash
pip install -e "../../.[scoring,vertex]"
```

Or using uv:

```bash
uv pip install -e "../../.[scoring,vertex]"
```

## Usage

```bash
# Build Gemini SFT JSONL for the echo dataset
python pipeline.py build --datasets echo --round-id 2026-06-01-echo

# Submit a Vertex AI Gemini SFT tuning job (requires --confirm)
python pipeline.py tune --round-id 2026-06-01-echo \
  --base-model gemini-3.1-flash-lite --confirm

# Run evaluation on the tuned Gemini model
python pipeline.py eval --round-id 2026-06-01-echo

# Full Gemini SFT pipeline: build -> tune -> eval
python pipeline.py all --datasets echo --round-id 2026-06-01-echo \
  --base-model gemini-3.1-flash-lite --confirm
```

## Datasets

The Gemini SFT pipeline is **Echo-only** - it fine-tunes Gemini on Watch Duty's
proprietary emergency-radio data. The `datasets.toml` registry registers one dataset
via the `gcs_manifest` adapter:

| Name   | Adapter      | License          | Notes |
|--------|--------------|------------------|-------|
| `echo` | gcs_manifest | Proprietary (WD) | `train_manifest_uri` requires the Phase 4 cluster-split script |

Note: The Echo `train_manifest_uri` in `datasets.toml` is a placeholder — it must be
populated after the cluster-split script runs (Phase 4 prerequisite, DESIGN.md #14).

(The earlier HuggingFace ATC augmentation datasets — atcosim / uwb_atcc / atco2 — and the
`hf_dataset` adapter were removed to keep the pipeline Echo-only.)

## Cost

- `tune` requires `--confirm` to prevent accidental paid runs.
- The `$55-175/run` planning ballpark came from the Gemini 3.1 Flash-Lite supervised fine-tuning
  rate: training tokens = dataset tokens x epochs, priced at $3 per 1M training tokens
  at the time of the estimate. That implies roughly 18-58M billable training tokens for
  the planned Echo run.
- Recompute before running with the current
  [Vertex AI pricing](https://cloud.google.com/vertex-ai/generative-ai/pricing), because
  Gemini SFT prices vary by model and can change after the README is committed.
- The `--confirm` gate displays an estimated cost before proceeding.

## Records

Per-run records are written to `results/<round-id>/`:

- `config.json` — parameters, dataset URIs, job name, tuned model endpoint
- `wer_summary.{md,json}` — evaluation metrics (base WER, tuned WER, delta, bootstrap CI)
- `results/ledger.md` — one-row-per-run summary table

Built training JSONL files are NOT git-committed (D-16 governance — they contain proprietary
Watch Duty Echo transcripts).

## Prompt Parity

`prompts.py` re-exports `PIPELINE_SYSTEM_PROMPT` / `PIPELINE_USER_PROMPT` from
`common.prompts` (`GEMINI_TRANSCRIBE_*`) — the single canonical source it shares with
`model/colabs/gemini_transcribe_audio.ipynb` and the `eval` stage's request builder
(`common.vertex.build_request`). A drift-guard test asserts both sides import from
`common`, so the prompt and inference setup can't silently diverge.
