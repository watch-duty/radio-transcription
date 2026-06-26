# Gemini Prior-Context SFT Protocol

## Objective

Fine-tune `gemini-3.1-flash-lite` for radio transcription with prior conversation
context. The confirmatory run uses up to 8 prior conversations from the same
source recording as the current clip.

This run follows the multi-turn setup used by:

- `model/colabs/evaluate_gemini_manual_context.ipynb`
- `model/colabs/gemini_agent_session/create_inference_manifest_masked_audio.ipynb`
- `model/colabs/gemini_agent_session/transcribe_masked_audio.ipynb`

## Hypothesis

Supplying the previous 8 same-source audio/transcript turns during SFT will
improve transcription WER on the v20260528 eval set versus the base model,
because recent radio traffic provides local unit IDs, incident context, and
speaker/channel vocabulary that are not always acoustically clear in the
current short clip.

## Data

Training manifests:

- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_calls/train.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_feeds/train.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/echo/train.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/fire_notifications/train.jsonl`

Validation and scorer eval manifests:

- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_calls/eval.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_feeds/eval.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/echo/eval.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/fire_notifications/eval.jsonl`

The eval set is copied into both `validation` and `eval` canonical manifests.
Training must remain disjoint from both validation and eval by target audio URI
and canonical identity.

## Context Construction

For each row, define a source/session key using `original_audio_uri`, with
fallbacks to source audio metadata and then canonical audio fields. Sort rows
within a source by `original_offset`, then row order metadata.

Each example uses at most the previous 8 successful same-source rows. A row is
eligible for future history when its transcript is non-empty and not exactly
`[UNINTELLIGIBLE]`.

Gemini contents schema:

1. For each prior turn: `user(audio only)` followed by `model(gold transcript)`.
2. For the current clip: `user(text TURN_PROMPT, current audio)`.
3. Final target: `model(current gold transcript)`.

The current prompt is the manual-context notebook command:

```text
COMMAND: The preceding audio turns are for situational awareness ONLY. DO NOT re-transcribe them.
Execute strict verbatim transcription EXCLUSIVELY on the single audio clip attached to this specific message.
Apply all CRITICAL RULES.
```

This is teacher-forced prior context: validation/eval use gold prior transcripts,
not self-fed predicted transcripts.

## SFT Settings

- Base model: `gemini-3.1-flash-lite`
- Adapter size: `SIXTEEN`
- Learning rate multiplier: `0.5`
- Epoch count: `8`
- GCP location: `us-central1`
- Artifact prefix: `gs://wd-transcription-data/sft/runs/<round_id>`

## Execution

1. Build combined canonical train, validation, and eval manifests under
   `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/manifests/`.
2. Run `gemini-sft prepare` with `[context] prior_turn_count = 8`.
3. Inspect prepared Gemini JSONL to verify second and later same-source rows
   contain prior audio/model turns before the current clip.
4. Run `gemini-sft tune --confirm`.
5. Run `gemini-sft eval` to batch-score base and tuned models with the same
   count-8 prior context.

## Acceptance Checks

- Prepare preflight passes.
- Vertex tuning job succeeds and writes a tuned endpoint to `config.json`.
- Batch scorer completes and writes WER/CER summaries.
- The selected checkpoint/endpoint is accepted only if tuned WER improves over
  the context-aware base evaluation, or if the result documents a clear negative
  finding for this context-SFT design.
