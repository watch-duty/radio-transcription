# Watch Duty Radio Transcription SFT Pipeline

A re-runnable pipeline for supervised fine-tuning (SFT) of Watch Duty's emergency-radio
transcription model on Vertex AI.

## Subcommands

```
python pipeline.py build   Build SFT JSONL from registered datasets
python pipeline.py tune    Submit Vertex AI SFT tuning job (--confirm required; ~$55-175/run)
python pipeline.py eval    Batch-infer and score a model on the held-out manifest
python pipeline.py all     build -> tune -> eval in one invocation
```

## Installation

From this directory (`model/scripts/sft/`):

```bash
pip install -e "../../.[scoring,hf,audio,vertex]"
```

Or using uv:

```bash
uv pip install -e "../../.[scoring,hf,audio,vertex]"
```

## Usage

```bash
# Build SFT JSONL for echo + atcosim datasets
python pipeline.py build --datasets echo,atcosim --round-id 2026-06-01-echo-atcosim

# Submit a tuning job (requires --confirm to prevent accidental runs)
python pipeline.py tune --round-id 2026-06-01-echo-atcosim \
  --base-model gemini-2.5-flash --confirm

# Run evaluation on the tuned model
python pipeline.py eval --round-id 2026-06-01-echo-atcosim

# Full pipeline: build -> tune -> eval
python pipeline.py all --datasets echo,atcosim --round-id 2026-06-01-echo-atcosim \
  --base-model gemini-2.5-flash --confirm
```

## Datasets

The `datasets.toml` registry registers four datasets:

| Name       | Adapter      | License            | Notes |
|------------|--------------|--------------------|-------|
| `echo`     | gcs_manifest | Proprietary (WD)   | train_manifest_uri requires Phase 4 cluster-split script |
| `atcosim`  | hf_dataset   | CC-BY-NC-SA-4.0    | 7,650 train / 1,910 val examples |
| `uwb_atcc` | hf_dataset   | CC-BY-NC-SA-4.0    | 8 kHz audio; auto-resampled to 16kHz; confirm NonCommercial before prod |
| `atco2`    | hf_dataset   | CC-BY-NC-SA-4.0    | 446 train / 113 val examples |

Note: The Echo `train_manifest_uri` in `datasets.toml` is a placeholder — it must be
populated after the cluster-split script runs (Phase 4 prerequisite, DESIGN.md #14).

## Cost

- `tune` requires `--confirm` to prevent accidental paid runs (~$55-175 per run at
  Gemini 2.0 Flash rates; actual 2.5 Flash SFT rate is not publicly listed)
- The `--confirm` gate displays an estimated cost before proceeding

## Records

Per-run records are written to `results/<round-id>/`:

- `config.json` — parameters, dataset URIs, job name, tuned model endpoint
- `wer_summary.{md,json}` — evaluation metrics (base WER, tuned WER, delta, bootstrap CI)
- `results/ledger.md` — one-row-per-run summary table

Built training JSONL files are NOT git-committed (D-16 governance — may contain
NonCommercial UWB-ATCC data and proprietary Watch Duty Echo transcripts).

## Prompt Parity

The `prompts.py` module contains `PIPELINE_SYSTEM_PROMPT` seeded byte-for-byte from
`model/colabs/gemini_transcribe_audio.ipynb` (D-06 hard constraint). The drift-guard
test (`tests/test_prompt_parity.py`) asserts this parity on every run.
