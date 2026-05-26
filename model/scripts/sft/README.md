# Watch Duty Radio Transcription Gemini SFT Pipeline

A re-runnable pipeline for supervised fine-tuning (SFT) of Watch Duty's
emergency-radio transcription model on Vertex AI Gemini.

## Subcommands

```
python pipeline.py build   Build SFT JSONL from registered datasets
python pipeline.py tune    Submit Vertex AI SFT tuning job (--confirm required; ~$55-175/run)
python pipeline.py eval    Batch-infer and score a model on the held-out manifest
python pipeline.py all     build -> tune -> eval in one invocation
```

## Runtime

Preferred local runtime is the repo's lightweight ASR experiment Docker service. It
mounts the repo at `/workspace` and bootstraps the local `common` package as
`/workspace/model[scoring,vertex]` on container startup.

From the repo root:

```bash
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'cd /workspace/model/scripts/sft && python pipeline.py --help'
```

Use `notebooks-cpu` for Gemini SFT CLI work. The paid tune/eval jobs run remotely on
Vertex AI, so no local GPU is required. The `notebooks` service remains available for
GPU-backed notebook workflows that need it.

## Local Installation

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
# Build SFT JSONL for the echo dataset
python pipeline.py build --datasets echo --round-id 2026-06-01-echo

# Submit a tuning job (requires --confirm to prevent accidental runs)
python pipeline.py tune --round-id 2026-06-01-echo \
  --base-model gemini-2.5-flash --confirm

# Run evaluation on the tuned model
python pipeline.py eval --round-id 2026-06-01-echo

# Full pipeline: build -> tune -> eval
python pipeline.py all --datasets echo --round-id 2026-06-01-echo \
  --base-model gemini-2.5-flash --confirm
```

## Datasets

The pipeline is **Echo-only** — it fine-tunes on Watch Duty's proprietary emergency-radio
data. The `datasets.toml` registry registers one dataset via the `gcs_manifest` adapter:

| Name   | Adapter      | License          | Notes |
|--------|--------------|------------------|-------|
| `echo` | gcs_manifest | Proprietary (WD) | `train_manifest_uri` requires the Phase 4 cluster-split script |

Note: The Echo `train_manifest_uri` in `datasets.toml` is a placeholder — it must be
populated after the cluster-split script runs (Phase 4 prerequisite, DESIGN.md #14).

(The earlier HuggingFace ATC augmentation datasets — atcosim / uwb_atcc / atco2 — and the
`hf_dataset` adapter were removed to keep the pipeline Echo-only.)

## Cost

- `tune` requires `--confirm` to prevent accidental paid runs (~$55-175 per run at
  Gemini 2.0 Flash rates; actual 2.5 Flash SFT rate is not publicly listed)
- The `--confirm` gate displays an estimated cost before proceeding

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
