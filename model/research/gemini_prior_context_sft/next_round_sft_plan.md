# Next Round Gemini Prior-Context SFT Plan

## Objective

Recover or beat the old corrected checkpoint on the v20260528 four-dataset eval
set while preserving useful prior-conversation context.

Primary target:

- WER at or below `22.34`, the old corrected checkpoint score on the same eval
  rows.
- Empty response rate at or below `0.5%`.
- No inference fallback in the confirmatory metric.

Secondary target:

- If the primary target is not reached, the next round must at least beat the
  best no-fallback prompt-only mitigation for the count-8 checkpoint:
  WER `23.17`, CER `16.59`, empty rows `53/4108`.

## Current Findings To Preserve

The count-8 text-turn SFT run was strong on rows where it produced text, but it
learned an immediate-stop failure mode:

- checkpoint 7 full eval: WER `27.63`, CER `22.67`, empty rate `6.69%`
- checkpoint 7 non-empty-row subset WER: `21.55`
- old corrected checkpoint non-empty-row subset WER: `21.66`
- replacing checkpoint-7 empty rows with old checkpoint outputs gives full WER
  `22.24`

Settings probes ruled out safety blocking, output format, token limit, automatic
function calling, and thinking budget as primary causes. The useful
no-fallback mitigation was prompt/schema-level: represent prior transcripts as
one context block in the current user turn, paired with a short ASR system
prompt and a current prompt that explicitly asks for words when speech is
present.

Best primary prompt variant so far:

- history presentation: one prior-transcript text block in the current user turn
- system prompt:

```text
You are a strict verbatim ASR system for VHF/UHF dispatch radio. Transcribe only the attached current audio clip. Use prior transcripts only as context for names, units, and terminology. Output only the transcript, with no explanation.
```

- current prompt:

```text
Transcribe the attached radio audio clip. Return the words spoken in this clip only. If speech is present, return words; do not stop before producing text.
```

## SFT Configuration Surface

Checked against the local `google-genai` SDK surface and Context7 docs for
`/googleapis/python-genai`.

### Meaningful Training Levers For This Project

These should be considered real experimental variables:

- `base_model`: fresh foundation model versus a tuned model resource for
  continuous tuning.
- `pre_tuned_model_checkpoint_id`: checkpoint to continue from when
  `base_model` is a tuned model resource.
- `training_dataset`: train JSONL content, including prompt schema, prior
  context construction, filtering, balancing, and sampling.
- `validation_dataset`: fixed to the requested eval manifests for checkpoint
  selection and comparable validation loss.
- `epoch_count`: total epochs in a fresh job, or additional epochs in a
  continuous-tuning job.
- `adapter_size`: available SDK enum includes `ONE`, `TWO`, `FOUR`, `EIGHT`,
  `SIXTEEN`, and `THIRTY_TWO`; the current repo wrapper exposes `ONE`, `TWO`,
  `FOUR`, `EIGHT`, `SIXTEEN`.
- `learning_rate_multiplier`: the correct Gemini first-party model LR knob;
  current wrapper validates `0.001` through `10.0`.
- `export_last_checkpoint_only`: should stay unset/false for this research,
  because selecting the best checkpoint is central to the workflow.
- `tuned_model_display_name`, `description`, and `labels`: useful for tracking,
  not expected to change quality.

### Prompt And Data Levers

These are likely higher leverage than low-level hyperparameters:

- Prior context representation:
  `text_turns`, transcript block in current user turn, no context, mixed context
  counts.
- Prior context count:
  keep the primary count at `8`; run count ablations only after the schema fix.
- Prior context source:
  same `original_audio_uri`/source segment grouping as the prior notebooks.
- Prior transcript text:
  gold prior transcripts for SFT train/validation, matching the current
  teacher-forced setup.
- System prompt:
  long domain prompt versus short strict-ASR prompt.
- Current prompt:
  original notebook command versus the best `force_words` current prompt.
- Context block wording:
  must match the prompt-probe best variant exactly, not merely use the existing
  generic `transcript` helper.
- Training mixture:
  pure count-8 examples versus mixed context counts such as `{0, 2, 4, 8}`.
- Row weighting/sampling:
  optional hard-slice oversampling if the fixed prompt schema still leaves many
  empty responses.

### SDK Fields Not Recommended As First-Round Quality Knobs

These exist in the SDK but should not be part of the first sweep:

- `learning_rate`: use `learning_rate_multiplier` for first-party Gemini SFT;
  do not set both.
- `batch_size`: exposed for some tuning modes/models, but not currently used by
  the wrapper and not a known quality lever for this Gemini SFT path.
- `evaluation_config`: useful if relying on Vertex-managed evals; external
  WER/CER scorer remains the source of truth.
- `output_uri`: artifact placement only.
- `encryption_spec`: admin/security setting only.
- `tuning_mode` / `custom_base_model`: not needed for the current first-party
  Gemini adapter SFT flow.
- RL-specific fields such as `reward_config`, `samples_per_prompt`,
  `evaluate_interval`, `checkpoint_interval`, `max_output_tokens`,
  `thinking_level`, and `beta`: not part of the supervised ASR SFT question.
- Inference-only settings such as safety thresholds, temperature, top-p/top-k,
  response MIME type, response modalities, audio timestamps, and thinking
  budget: already probed as inference mitigations; they should not define the
  training run.

## Required Implementation Before New SFT

Add an exact prompt-schema option for the best transcript-block variant:

1. One current user turn only for prior context, containing:
   - `Prior same-source transcripts for context only:`
   - numbered prior transcripts, oldest to newest
   - a blank line
   - the current `force_words` prompt
2. Exactly one audio part: the current audio clip.
3. The model target is the current gold transcript.
4. The same builder must be used for SFT train, SFT validation, and checkpoint
   evaluation.
5. Add tests that assert the prepared JSONL has one audio part and byte-matches
   the selected system/current/context-block text.

The existing `prior_context_mode = "transcript"` is close but not identical to
the best prompt-probe shape, so it should not be used as-is for the primary
confirmatory run.

## Experiment Matrix

### A. Confirmatory Schema Fix

Run this first and wait for checkpoint scores before launching any sweep.

Run id proposal:
`20260627-prior-context-count8-block-forcewords-a16-lr05-e8`

Configuration:

- base model: `gemini-3.1-flash-lite`
- continuous tuning: no
- prior context count: `8`
- prior context representation: exact transcript block from the best prompt
  probe
- system prompt: short strict-ASR prompt above
- current prompt: `force_words` prompt above
- adapter size: `SIXTEEN`
- learning-rate multiplier: `0.5`
- epoch count: `8`
- validation set: requested four eval manifests
- checkpoint export: all checkpoints retained
- scorer: no fallback, temperature `0.0`, max output tokens `512`,
  safety `OFF`/`BLOCK_NONE` as in the prompt probe

Prediction:

- WER should land below `23.17` because the model is trained on the prompt
  schema that already improved inference.
- Empty responses should be close to or below `1.5%`.

Decision:

- If any checkpoint reaches WER `<=22.34` and empty rate `<=0.5%`, accept it as
  the next best model.
- If best WER is `22.34-23.17`, run the LR/adapter refinement.
- If best WER is worse than `23.17`, stop and do error analysis before further
  SFT spending.

### B. Hyperparameter Refinement

Run only if A improves over prompt-only mitigation but does not beat the old
corrected checkpoint.

Keep the exact same data and prompt schema. Vary only one training knob at a
time.

| run | adapter | LR multiplier | epochs | rationale |
|---|---:|---:|---:|---|
| B1 | `SIXTEEN` | `0.25` | 8 | lower LR if A overfits or still has empty-stop behavior |
| B2 | `EIGHT` | `0.5` | 8 | smaller adapter may reduce schema-specific overfit |
| B3 | `SIXTEEN` | `0.75` | 8 | only if A looks underfit by validation/checkpoint trajectory |

Do not run B3 unless validation loss and checkpoint WER both suggest underfit;
the prior failures look more like behavior collapse than insufficient update
size.

### C. Context Robustness

Run only after the transcript-block schema has a successful checkpoint.

| run | context count policy | adapter | LR multiplier | epochs | rationale |
|---|---|---:|---:|---:|---|
| C1 | fixed count `4` | `SIXTEEN` | best from A/B | 8 | test whether count 8 adds avoidable noise |
| C2 | mixed counts `{0, 2, 4, 8}` | `SIXTEEN` | best from A/B | 8 | teach robustness to missing/short histories while preserving count-8 eval |
| C3 | count `0` baseline | `SIXTEEN` | best from A/B | 8 | isolate whether prior context is still adding value after schema fix |

Primary deployment candidate should still be scored with count `8` unless C1 or
C2 clearly wins on full WER and empty rate.

### D. Continuous Tuning

Do not continue from the current text-turn count-8 checkpoint for the main next
round. It carries the immediate-stop behavior that caused the regression.

Continuous tuning is useful only after A/B produces a clean transcript-block
checkpoint:

- continue from the best clean checkpoint for `+2` or `+4` epochs
- use the same dataset and prompt schema
- prefer LR multiplier `0.25` for continuation unless the parent checkpoint
  clearly underfits

The currently running `+4` continuous job from text-turn checkpoint 7 should be
scored as an exploratory negative/positive control, not treated as the primary
path.

## Evaluation Protocol

For every completed SFT job:

1. Score all exposed checkpoints on the same four eval manifests.
2. Use no fallback in the primary metric.
3. Report WER, CER, empty rate, per-dataset WER/CER, and checkpoint epoch/step.
4. Recompute:
   - the 275-row old empty-response slice
   - the full eval excluding empty outputs
   - rows where the old corrected checkpoint was better by a large margin
5. Compare against:
   - base model
   - old corrected checkpoint WER `22.34`, CER `15.46`, empty `0.27%`
   - current checkpoint-7 best prompt WER `23.17`, CER `16.59`, empty `53/4108`

Only launch the next stage after reviewing these scorer results. The experiment
budget should go to prompt-schema correctness first, not broad hyperparameter
sweeps.
