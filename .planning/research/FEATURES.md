# Research: Features

## Table Stakes

- Deterministic dataset version ID and GCS artifact tree.
- Input manifest registry/config that names dataset family, manifest URI, parser mode, and source-key strategy.
- Source-key extraction for Broadcastify Calls, Broadcastify Feeds, Echo, and Fire Notifications.
- Hard leakage validation:
  - no source group in both train and SFT Eval Split
  - no original source audio in both splits
  - no duplicate model-ready audio URI in both splits
  - fail on unresolved source identity
  - fail when a configured dataset has no valid examples
- Seeded 80:20 split at the source-group level.
- Balance report across variables likely to correlate with performance:
  - dataset family
  - source count
  - row/example count
  - total duration
  - month/date/hour where available
  - duration bucket
  - transcript length bucket
  - missing/empty transcript counts
- Canonical train/eval JSONL plus per-dataset slices.
- Model-specific inputs for NeMo, Whisper, and Gemini.
- GCS-first output layout with no generated artifacts committed to Git.
- Provenance metadata for every example.
- Dry-run mode that validates and reports without uploading derived audio.
- Existing path protection: fail when a dataset version already exists unless an explicit force flag is provided.

## Differentiators

- Candidate-search split optimizer that preserves hard leakage gates while minimizing imbalance across multiple correlated dimensions.
- Source-key failure report that explains exactly why a row failed and which fallback was attempted.
- Transformation manifest that distinguishes reused audio from derived clips and records codec/sample-rate/channel decisions.
- Report bundle that is directly reviewable before model runs: Markdown summary plus machine-readable JSON.
- Per-model preflight validation before writing final model input manifests.

## Anti-Features

- Random row-level splitting. This is the main leakage trap.
- Treating Echo `echo_name` alone as a source key. It is ambiguous across area codes.
- Treating Fire Notification day UUID as the source key. That is a sampling artifact and would allow the same stream across splits.
- Automatically rewriting `model/scripts/sft/datasets.toml` for every generated dataset version. Generated run configs should live with the dataset artifact tree.
- Using SFT Eval Split terminology as hidden holdout. It may be used for validation/selection and must be named honestly.

## Suggested User-Facing Outputs

```text
gs://wd-transcription-data/sft/{dataset_version_id}/
  canonical/train.jsonl
  canonical/eval.jsonl
  canonical/by_dataset/{dataset}/train.jsonl
  canonical/by_dataset/{dataset}/eval.jsonl
  audio/{dataset}/{split}/...
  model_inputs/nemo/train.jsonl
  model_inputs/nemo/eval.jsonl
  model_inputs/nemo/config.yaml
  model_inputs/whisper/train.jsonl
  model_inputs/whisper/eval.jsonl
  model_inputs/whisper/dataset_config.json
  model_inputs/gemini/train.jsonl
  model_inputs/gemini/eval.jsonl
  model_inputs/gemini/tuning_config.json
  reports/split_summary.md
  reports/split_summary.json
  reports/leakage_report.json
  reports/balance_report.json
  reports/source_key_failures.jsonl
  reports/transformations.jsonl
```

## V2 Candidates

- Tarred/WebDataset NeMo output for large-scale training.
- Integrated Vertex custom-training job specs for NeMo/Whisper.
- Automated post-tuning eval job submission.
- UI/dashboard for comparing dataset versions and model runs.
