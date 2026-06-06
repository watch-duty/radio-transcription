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

# Submit one config-driven Vertex AI Gemini SFT tuning job
python pipeline.py tune --config /path/to/run.toml --confirm

# Run evaluation on the tuned Gemini model
python pipeline.py eval --round-id 2026-06-01-echo

# Full Gemini SFT pipeline: build -> tune -> eval
python pipeline.py all --datasets echo --round-id 2026-06-01-echo \
  --base-model gemini-3.1-flash-lite --confirm
```

`all --config` is intentionally unsupported in this milestone; use
`tune --config /path/to/run.toml` for config-driven runs.

## Config-Driven Tune

`tune --config` starts or resumes one supervised fine-tuning job from an
external TOML file. It does not read or require `datasets.toml`; upstream data
preparation must provide canonical train, validation, and eval manifests in GCS.
Real run configs are external run inputs and should not be committed. Commit
only placeholder examples such as `run_config.example.toml`.

```toml
round_id = "YYYY-MM-DD-short-description"
dataset = "dataset-version-name"
train_manifest_uri = "gs://your-bucket/path/manifests/canonical/train.jsonl"
validation_manifest_uri = "gs://your-bucket/path/manifests/canonical/validation.jsonl"
eval_manifest_uri = "gs://your-bucket/path/manifests/canonical/eval.jsonl"

[gcp]
project = "your-gcp-project"
bucket = "your-gcs-bucket"
location = "us-central1"

[sft]
base_model = "gemini-3.1-flash-lite"
epoch_count = 6
adapter_size = "SIXTEEN"
learning_rate_multiplier = 1.0

[prompts]
# Optional inline overrides only.
# system = "..."
# user = "..."
```

Supported adapter sizes are: `ONE, TWO, FOUR, EIGHT, SIXTEEN`.

## Datasets

The Gemini SFT pipeline is **Echo-only** - it fine-tunes Gemini on Watch Duty's
proprietary emergency-radio data. The `datasets.toml` registry registers one dataset
via the `gcs_manifest` adapter:

| Name   | Adapter      | License          | Notes |
|--------|--------------|------------------|-------|
| `echo` | gcs_manifest | Proprietary (WD) | `train_manifest_uri` requires the cluster-split script |

Note: The Echo `train_manifest_uri` in `datasets.toml` is a placeholder — it must be
populated after the cluster-split script runs.

(The earlier HuggingFace ATC augmentation datasets — atcosim / uwb_atcc / atco2 — and the
`hf_dataset` adapter were removed to keep the pipeline Echo-only.)

## Cost

- `tune` prompts for confirmation unless `--confirm` is passed to prevent accidental paid runs.
- Use `--confirm` in non-interactive automation; otherwise the command waits for stdin.
- The `$55-175/run` planning ballpark came from the Gemini 3.1 Flash-Lite supervised fine-tuning
  rate: training tokens = dataset tokens x epochs, priced at $3 per 1M training tokens
  at the time of the estimate. That implies roughly 18-58M billable training tokens for
  the planned Echo run.
- Recompute before running with the current
  [Vertex AI pricing](https://cloud.google.com/vertex-ai/generative-ai/pricing), because
  Gemini SFT prices vary by model and can change after the README is committed.
- The `--confirm` gate displays an estimated cost before proceeding.

## Records

Legacy `build`, `tune`, and `eval` write per-run records to `results/<round-id>/`:

- `config.json` — parameters, dataset URIs, job name, tuned model endpoint
- `wer_summary.{md,json}` — evaluation metrics (base WER, tuned WER, delta, bootstrap CI)
- `results/ledger.md` — one-row-per-run summary table

For config-driven tune, authoritative records are written to
`gs://<bucket>/sft/runs/<round-id>/`; local results/<round-id>/ is a mirror/cache only.

```text
gs://<bucket>/sft/runs/<round-id>/
  run_config.toml
  config.json
  status.json
  manifests/canonical/train.jsonl
  manifests/canonical/validation.jsonl
  manifests/canonical/eval.jsonl
  model_inputs/gemini/train.jsonl
  model_inputs/gemini/validation.jsonl
  preflight/report.json
  tuning/status.json
  evals/README.txt
```

Built training JSONL files are NOT git-committed because they contain proprietary Watch
Duty Echo transcripts.

## Prompt Parity

`prompts.py` re-exports `PIPELINE_SYSTEM_PROMPT` / `PIPELINE_USER_PROMPT` from
`common.prompts` (`GEMINI_TRANSCRIBE_*`) — the single canonical source it shares with
`model/colabs/gemini_transcribe_audio.ipynb` and the `eval` stage's request builder
(`common.vertex.build_request`). A drift-guard test asserts both sides import from
`common`, so the prompt and inference setup can't silently diverge.
