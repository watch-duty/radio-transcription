# Research: Stack

## Scope

Domain: leak-safe supervised fine-tuning dataset versioning for emergency-radio ASR across NeMo, Whisper, and Gemini on Vertex AI.

This is a brownfield project. The implementation should extend `model/scripts/sft` and `model/colabs/common`, not introduce a separate training system.

## Existing Local Stack

- Python 3.13 with `uv` is the primary implementation environment.
- GCS is already the artifact store for audio, inference manifests, and SFT build outputs.
- Existing model tooling lives in `model/scripts/sft` and shared helpers in `model/colabs/common`.
- `common.manifest.CanonicalRow` is the current per-segment contract: `audio_filepath`, `text`, `offset`, and `duration`.
- `GcsManifestAdapter` already reads pre-split GCS JSONL manifests and yields `CanonicalRow` objects.
- `common.sft.build_example` currently builds Gemini-style JSONL with `fileData` and prompt text, but is audio/format-specific and should be generalized carefully.
- Existing `_tune` and `_eval` bodies in `model/scripts/sft/pipeline.py` are stubs; this project should generate model inputs/configs first, not make training execution the dependency.

## External Input Requirements

### NeMo ASR

NVIDIA NeMo expects custom ASR datasets to be utterance-level audio files plus a manifest with one JSON object per line. The documented row shape is:

```json
{"audio_filepath": "/path/to/audio.wav", "text": "the transcription of the utterance", "duration": 23.147}
```

NeMo documents one manifest per dataset/split and passing paths through ASR config fields such as `model.train_ds.manifest_filepath` and `model.validation_ds.manifest_filepath`. WAV is the recommended safest output format even though Pydub-supported formats may load.

Implication: generate `model_inputs/nemo/train.jsonl`, `model_inputs/nemo/eval.jsonl`, and a companion config fragment that points train and validation manifests at those files.

Sources:
- NVIDIA NeMo ASR datasets: https://docs.nvidia.com/nemo-framework/user-guide/latest/nemotoolkit/asr/datasets.html
- NVIDIA NeMo ASR config: https://docs.nvidia.com/nemo-framework/user-guide/latest/nemotoolkit/asr/configs.html

### Whisper

The practical fine-tuning path is Hugging Face Transformers/Datasets. The documented preparation flow uses a dataset with an audio column and a transcription column, casts audio to the Whisper processor sampling rate, builds processor outputs from audio arrays and text, records input length, and filters samples longer than 30 seconds because the feature extractor would otherwise truncate them.

Implication: generate a Whisper JSONL/Parquet-friendly manifest with `audio_filepath`, `text`, `duration`, and stable IDs. The training script can load audio through `datasets.Audio`, resample on load to `processor.feature_extractor.sampling_rate`, and enforce a hard duration cap for examples intended for Whisper.

Source:
- Hugging Face Audio Course Whisper fine-tuning: https://huggingface.co/learn/audio-course/chapter5/fine-tuning

### Gemini Supervised Tuning on Vertex AI

Current Gemini Enterprise Agent Platform documentation for supervised tuning supports Gemini 3.1 Flash-Lite, Gemini 2.5 Pro, Gemini 2.5 Flash, and Gemini 2.5 Flash-Lite. The older Vertex AI page previously checked had a stale supported-model list, so implementation should treat the Agent Platform model list as the current source of truth while keeping the base model configurable.

Gemini SFT datasets are JSONL files in Cloud Storage. Each line is a tuning example with optional `systemInstruction` and required `contents`; `parts` may contain text or `fileData` with `mimeType` and `fileUri`. Vertex tuning accepts training and optional validation dataset GCS URIs; the SDK examples use `TuningDataset(gcs_uri=...)` and `validation_dataset`.

Relevant limits for Gemini 2.5 Flash/Flash-Lite: JSONL training file max 1 GB, validation max 5,000 examples or 30% of training examples when validation has more than 1,000 examples, and multimodal training size max 300K examples.

Implication: generate `model_inputs/gemini/train.jsonl`, `model_inputs/gemini/eval.jsonl`, and a tuning config containing model name, training dataset URI, validation dataset URI, region, adapter size, epoch count, and learning-rate multiplier. Keep the prompt shape consistent with production/eval prompts.

Sources:
- Prepare Gemini SFT data: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning-prepare
- Use Gemini supervised tuning: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-use-supervised-tuning
- Gemini SFT supported models/limits: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning
- Gemini Enterprise Agent Platform supervised tuning supported models: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-use-supervised-tuning

## Recommended Implementation Stack

- CLI module under `model/scripts/sft` for dataset-version generation.
- Shared pure-Python helpers under `model/colabs/common` for source-key extraction, split planning, manifest normalization, and validation reports.
- JSONL as the canonical row format and report-friendly JSON/Markdown summaries.
- GCS path planner using existing `common.gcs_utils` style.
- Focused pytest coverage in `model/scripts/sft/tests` and `model/colabs/common/tests`.

## What Not To Add

- Do not introduce a new orchestration framework for this phase.
- Do not make Vertex job submission a prerequisite for dataset generation.
- Do not depend on notebooks for the core splitter.
- Do not commit generated audio or proprietary manifests into Git.
