# Research Summary

## Key Findings

**Stack:** Extend the existing Python SFT/model tooling under `model/scripts/sft` and `model/colabs/common`. Use JSONL, GCS, and pytest. Do not introduce a new orchestration stack for the splitter.

**Model inputs:** NeMo wants separate ASR manifests per split with `audio_filepath`, `text`, and `duration`; Whisper should receive loader-friendly audio/text manifests with duration filtering and 16 kHz on-load resampling handled by the training script; Gemini SFT wants JSONL examples in GCS with `contents` and optional `systemInstruction`, and supports optional validation JSONL.

**Data leakage:** Source Group is the correct split unit. Row-level, file-level, or day-sampling-group splits are insufficient for radio data because upstream source identity correlates with acoustics, agency/channel vocabulary, scanner setup, speaker population, and incident style.

**Echo:** `area_code` and `echo_name` matter because `echo_name` alone is not globally unique. Rows that cannot resolve a unique `area_code/echo_name` should fail source-key validation.

**Gemini target:** Current Google docs list Gemini 2.5 Pro, 2.5 Flash, and 2.5 Flash-Lite for supervised tuning. They do not list "Gemini 3.1 Flash-Lite"; keep base model configurable and validate it when a tuning job is submitted.

## Recommended Roadmap Shape

1. Normalize manifests and source identities.
2. Implement deterministic source-group split and hard leakage validation.
3. Write canonical GCS artifact layout and reports.
4. Generate model-specific NeMo, Whisper, and Gemini inputs/configs.
5. Add audio derivation/provenance execution and end-to-end CLI verification.

## Non-Negotiable Gates

- No source-group overlap between train and SFT Eval Split.
- No original-audio overlap between train and SFT Eval Split.
- No duplicate model-ready audio URI across splits.
- Fail unresolved source-key ambiguity.
- Fail configured datasets with zero valid examples.
- Fail existing dataset version path unless explicitly forced.

## Primary References

- NVIDIA NeMo ASR datasets: https://docs.nvidia.com/nemo-framework/user-guide/latest/nemotoolkit/asr/datasets.html
- NVIDIA NeMo ASR config: https://docs.nvidia.com/nemo-framework/user-guide/latest/nemotoolkit/asr/configs.html
- Hugging Face Whisper fine-tuning: https://huggingface.co/learn/audio-course/chapter5/fine-tuning
- Vertex Gemini SFT data prep: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning-prepare
- Vertex Gemini SFT job creation: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-use-supervised-tuning
- Vertex Gemini SFT models/limits: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning
