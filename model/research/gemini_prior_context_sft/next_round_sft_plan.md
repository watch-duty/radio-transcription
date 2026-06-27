# Next Round Gemini Prior-Context SFT Plan

## Latest Optimized One-SFT Plan

This is the current execution plan after the 2026-06-27 user-seeded VAPO
sweep. It supersedes the older P3-clean plan in this document.

### Decision

Run exactly one fresh SFT:

`20260627-prior-context-count8-p13-a16-lr075-e10`

Do not run `P12` or `P14` SFTs, do not continue from the empty-prone text-turn
checkpoint, and do not start a hyperparameter sweep before scoring this run.

### Why This Candidate

The latest VAPO sweep used the fixed `P3_manual_quality_gate` user-template
shape and compared the three user-provided system prompts. The metric is
`asr_wer_score`, where higher is better because the custom metric maps WER to a
bounded score before Prompt Optimizer maximizes it.

| rank | family | step | `asr_wer_score/mean` | decision |
|---:|---|---:|---:|---|
| 1 | `P13_user_forensic_context` | 7 | 0.410378 | use for SFT |
| 2 | `P12_user_clean_context` | 0 | 0.393718 | do not train |
| 3 | `P14_user_dispatch_glossary` | 9 | 0.373961 | do not train |

Artifact:

`gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-user-system-prompts-p3-template-g31-optglobal/outputs/P13_user_forensic_context/instruction/optimized_results.json`

The selected prompt supports prior context, but only as a constrained written
form/spelling reference for names, units, locations, or specialized terms that
are already clearly heard in the current audio. It avoids the long hard-coded
vocabulary list and uses `[UNINTELLIGIBLE]` rather than an empty-string
protocol for no intelligible speech.

### Hypothesis

Training the model directly on the P13 transcript-block prompt schema will
preserve the strong non-empty-row transcription quality seen in the prior
count-8 SFT while removing the immediate-empty failure mode caused by the older
text-turn schema.

Expected result:

- WER below `23.17`, the best no-fallback prompt-only mitigation.
- Empty response rate below `1.5%`.
- Stretch target: WER `<=22.34` and empty response rate `<=0.5%`, matching or
  beating the old corrected checkpoint.

### Fixed Data Setup

Training manifests:

- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_calls/train.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_feeds/train.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/echo/train.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/fire_notifications/train.jsonl`

Validation and final eval manifests:

- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_calls/eval.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/bcfy_feeds/eval.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/echo/eval.jsonl`
- `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/per_dataset/fire_notifications/eval.jsonl`

Use the existing combined canonical manifests under:

`gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/manifests/`

Regenerate Gemini train/validation JSONL under the new run prefix so the
resolved prompt text and context schema are stored with this run.

### Prompt Schema

Use exactly one current user turn with:

1. A transcript-only prior-context block.
2. Up to `8` prior same-source transcripts, oldest to newest.
3. The fixed current-audio instruction.
4. Exactly one audio part: the current clip.

Prior transcripts must be selected with the same source-grouping logic as the
manual-context notebooks and existing `build_context_histories` helper:

- group by `original_audio_uri` or equivalent source recording/session key
- sort by source offset
- include only non-empty prior transcripts that are not `[UNINTELLIGIBLE]`
- no prior audio in SFT context

The user template must byte-match the VAPO gate shape:

```text
The following prior same-source transcripts are for situational awareness only.
Do not re-transcribe them. Do not continue them.
Transcribe exclusively the current audio clip.

Prior transcripts, oldest to newest:
{prior_context}

IMPORTANT: The current audio is the only clip to transcribe. Use prior transcripts only as context. Return one line: the verbatim transcript of the attached radio audio clip.

Current audio clip:
```

If there is no prior context, use the same no-history sentence used by the VAPO
sample builder:

```text
There are no prior transcripts for this original recording.
```

### Required Preflight Implementation

Before launching SFT, add or verify an exact VAPO-template context mode. The
current `prior_context_mode = "transcript"` path is not sufficient because its
hard-coded header differs from the P3/P13 VAPO template.

Implemented mode for this run: `prior_context_mode = "vapo_p3_transcript"`.

Required checks:

- Prepared Gemini SFT examples contain exactly one audio part.
- Prior turns contain text only, no prior audio.
- The current user prompt byte-matches the VAPO template above for both
  history and no-history rows.
- The stored `config.json` contains the exact optimized P13 system prompt from
  `optimized_results.json`.
- Train rows do not overlap validation/eval by target audio URI.

### Tuning Configuration

- base model: `gemini-3.1-flash-lite`
- tuning type: fresh SFT, not continuous tuning
- adapter size: `SIXTEEN`
- learning-rate multiplier: `0.75`
- epoch count: `10`
- export/checkpoint policy: keep all intermediate checkpoints
- tuned model display name: `wd-radio-sft-20260627-prior-context-count8-p13-a16-lr075-e10`
- GCS run prefix:
  `gs://wd-transcription-data/sft/runs/20260627-prior-context-count8-p13-a16-lr075-e10/`

Vertex docs confirm these are the relevant supervised-tuning knobs:
`training_dataset`, optional `validation_dataset`, `epoch_count`,
`learning_rate_multiplier`, and `adapter_size`; checkpoint export should remain
enabled so external WER scoring can select the best checkpoint.

### Execution

1. Create the run config:
   `model/research/gemini_prior_context_sft/run_20260627_prior_context_count8_p13_a16_lr075_e10.local.toml`
2. Prepare and preflight:
   `safe-run -- uv run --project model --extra all gemini-sft prepare --config model/research/gemini_prior_context_sft/run_20260627_prior_context_count8_p13_a16_lr075_e10.local.toml`
3. Inspect the preflight report and spot-check at least:
   - one row with zero history
   - one row with partial history
   - one row with full count-8 history
4. Submit tuning:
   `safe-run -- uv run --project model --extra all gemini-sft tune --config model/research/gemini_prior_context_sft/run_20260627_prior_context_count8_p13_a16_lr075_e10.local.toml --confirm`
5. Poll every 5 minutes.
6. Score all exported checkpoints with no fallback:
   `safe-run -- uv run --project model --extra all python model/scripts/sft/score_gemini_sft_checkpoints_online.py --config model/research/gemini_prior_context_sft/run_20260627_prior_context_count8_p13_a16_lr075_e10.local.toml --concurrency 16 --log-every 100 --sync-every 100`

### Evaluation Gate

Primary scorer settings:

- no fallback
- temperature `0.0`
- max output tokens `512`
- safety settings consistent with the prompt probes
- same prompt/context builder as SFT train and validation

Report for every checkpoint:

- overall WER, CER, empty response rate
- per-dataset WER/CER/empty rate
- old-empty-slice performance from the 275 rows where the prior count-8
  checkpoint returned empty
- non-empty-row subset WER
- large-regression examples against the old corrected checkpoint

Decision rule:

- Accept if best checkpoint has WER `<=22.34` and empty rate `<=0.5%`.
- Tentatively accept for further review if WER is `22.34-23.17` and empty rate
  is `<=1.5%`.
- Stop and do error analysis before any more SFT if WER is worse than `23.17`
  or empty rate exceeds `1.5%`.

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

Best primary prompt variant so far, used only as the seed for prompt
optimization:

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

## Vertex AI Prompt Optimizer Surface

Checked against the Vertex AI Prompt Optimizer docs:

- Data-driven optimizer:
  `https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/prompts/data-driven-optimizer`
- Few-shot optimizer:
  `https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/prompts/few-shot-optimizer`

Use Vertex AI Prompt Optimizer before the next SFT run. Keep the SFT data,
context count, adapter, LR multiplier, epoch count, train manifests, and
validation manifests unchanged. Only prompt text and prompt packaging are
allowed to change.

Important constraint: the data-driven VAPO optimizer directly optimizes system
instructions and/or demonstrations for a fixed prompt template. It does not
freely mutate every prompt-template token. Therefore, optimize the full prompt
package by running VAPO over a small set of candidate prompt-template families,
then select the best full package with the existing WER scorer.

Prompt package components to optimize or compare:

- System instruction.
- Current transcription command.
- Prior-context header and ordering text.
- Whether demonstrations are selected by the optimizer.
- Any fixed text around the audio placeholder.

Prompt package components to keep fixed:

- Exactly one current audio clip.
- Prior context is transcript-only, not prior audio.
- Prior transcripts come from the same source segment/session as the current
  clip.
- Count window remains `8`.
- Decoding remains no-fallback, temperature `0.0`, max output tokens `512`.

### Repo Prompt Inventory

Existing repo prompts provide stronger seeds than the prompt-probe variants
alone. Use them to define optimizer template families:

- `model/src/common/gemini/prompts.py` and
  `model/colabs/gemini_transcribe_audio.ipynb`:
  canonical Gemini prompt. It emphasizes VHF/UHF fire dispatch traffic, heavy
  jargon, every spoken word, digit grouping, unit formatting, no continuation,
  and full-clip `[UNINTELLIGIBLE]` only when the entire clip is unusable.
- `backend/pipeline/transcription/transcribers/prompts.py`,
  `model/colabs/evaluate_gemini_manual_context.ipynb`, and
  `model/colabs/gemini_agent_session/transcribe_masked_audio.ipynb`:
  production/manual-context prompt. It has the broader terminology list,
  including `DO`, patrol/rescue/station/personnel, medical/fire alarm terms,
  `code 1-4`, `code 33`, `10-15`, `10-91`, and a stricter quality gate:
  replace ambiguous portions with `[UNINTELLIGIBLE]` instead of guessing.
- `backend/pipeline/transcription/transcribers/chirp_prompt.txt` and
  `model/colabs/chirp_transcribe_audio.ipynb`:
  Chirp default-style prompt. It explicitly says to transcribe every spoken
  word, including conversational phrasing and incomplete sentences, while only
  transcribing intelligible speech.
- `backend/pipeline/transcription/transcribers/chirp_phrase_hints.txt`:
  clean phrase-hint inventory. Use it to generate a compact terminology block
  instead of copying a long prose prompt verbatim.
- `model/src/common/prompts.py` and
  `model/colabs/evaluate_ibm_granite.ipynb`:
  compact cross-model ASR prompt. It is short, direct, and may avoid the
  over-constrained behavior of longer prompts.
- `model/colabs/evaluate_gemma_3n_e2b_it.ipynb`:
  strict output-only style. Useful seed: output only transcribed words and do
  not explain audio quality. Do not keep its "if audio is empty, output
  nothing" clause because empty responses are the known failure mode.
- `model/colabs/evaluate_gemma_4_E2B_it.ipynb`:
  formatting-first style. Useful seed: original-language transcription,
  no-newlines, and numeric formatting. It places text before audio.
- `model/colabs/evaluate_canary_qwen.ipynb`:
  minimal dispatch-specific user prompt. Useful as a terse baseline:
  "emergency dispatch radio traffic."
- `model/scripts/run_gemini_transcript_context_probe.py`:
  strongest context-block wording found outside SFT. It explicitly says prior
  transcripts may contain errors, not to re-transcribe/copy/continue them, and
  to use them only for recurring units, locations, and jargon.
- `model/scripts/sft/probe_empty_response_prompts.py`:
  measured prompt-probe variants. Keep the best `block_system_short_force_words`
  package as the incumbent seed and regression baseline.

### Prompt Optimizer Metric

Use a custom ASR metric rather than a generic standard metric. Standard metrics
such as coherence, fluency, groundedness, summarization quality, and QA quality
do not measure normalized radio-transcription WER and can prefer polished text
over verbatim ASR output.

Custom metric contract:

- Input: model response and target transcript.
- Normalization: same normalizer as the scorer.
- Score direction: higher is better, matching Prompt Optimizer requirements.
- Proposed per-example score:
  `1.0 - min(1.5, utterance_wer) / 1.5 - 0.5 * empty_response`.
- Empty responses are explicitly penalized even when the reference is short.
- Full WER/CER/empty rate from the existing scorer remains the final selection
  criterion; the optimizer metric is only a search objective.

### Prompt Optimizer Data

Build the optimizer sample set from the training manifests, not from the final
eval manifests, to avoid optimizing directly on the validation/eval set used
for checkpoint selection.

Recommended sample:

- 600 train examples total.
- Stratified by dataset: `bcfy_calls`, `bcfy_feeds`, `echo`,
  `fire_notifications`.
- Stratified by audio duration and prior-history count.
- Include rows with a full count-8 history when available.
- Include train examples that match the empty-response failure profile: short
  clips, noisy clips, and examples with dense prior context.

Hold out the four eval manifests for:

- scoring the optimizer's top prompt packages before SFT
- Vertex SFT validation
- final checkpoint scoring

### Prompt Template Families

Run VAPO independently for each template family, because template wording is a
first-class variable in this ASR task.

| family | current prompt seed | prior-context seed | optimizer mode |
|---|---|---|---|
| P1 | measured `force_words` incumbent | prompt-probe header | `instruction` |
| P2 | canonical Gemini shared prompt | generic transcript-block header | `instruction` |
| P3 | production/manual quality-gate prompt | manual-context warning style | `instruction` |
| P4 | Chirp default-style prompt plus compact phrase hints | generic transcript-block header | `instruction` |
| P5 | compact cross-model ASR prompt | generic transcript-block header | `instruction` |
| P6 | strict output-only Gemma-style prompt, with no empty-output clause | generic transcript-block header | `instruction` |
| P7 | transcript-context probe current task | "prior transcripts may contain errors" header | `instruction` |
| P8 | no-empty command with `[UNINTELLIGIBLE]` guidance | prompt-probe header | `instruction` |
| P9 | best P1-P8 package | best P1-P8 header | `instruction_and_demo` |

Do not start with fully unconstrained prompt mutation. P1 is the measured
incumbent. P2-P8 cover the existing prompt styles in the repo while keeping the
non-prompt SFT setup fixed. P9 tests whether selected demonstrations help, but
it should be rejected if demonstrations make SFT examples too long, make outputs
less verbatim, or increase hallucination risk.

## Required Implementation Before New SFT

Add a prompt-optimizer workflow and an exact prompt-schema option for the
optimizer-selected transcript-block variant:

1. Add a dedicated prompt-optimizer script or notebook.
   - The current model environment has `google-genai` but not the
     `vertexai`/Agent Platform SDK imports used by Prompt Optimizer, so add the
     required dependency to an optional experiment extra or run it in a
     dedicated Colab/Workbench environment.
   - Write optimizer configs and outputs under
     `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/`.
2. Implement the custom ASR metric endpoint or scorer adapter required by VAPO.
3. Build the 600-row stratified optimizer sample set from train manifests.
4. Run P1-P8 first with `optimization_mode = "instruction"`.
5. Run P9 only if the best instruction-only package still trails the current
   prompt-probe result on eval or if VAPO reports a clear demo-selection gain.
6. Score the top optimizer prompt packages on the full four-dataset eval set
   with no fallback.
7. Freeze the winning prompt package into the SFT run config.
8. One current user turn only for prior context, containing:
   - the optimizer-selected prior-context header
   - numbered prior transcripts, oldest to newest
   - a blank line
   - the optimizer-selected current prompt
9. Exactly one audio part: the current audio clip.
10. The model target is the current gold transcript.
11. The same builder must be used for SFT train, SFT validation, and checkpoint
    evaluation.
12. Add tests that assert the prepared JSONL has one audio part and byte-matches
    the selected system/current/context-block text.

The existing `prior_context_mode = "transcript"` is close but not identical to
the measured prompt-probe shape and may differ from the optimizer winner, so it
should not be used as-is for the primary confirmatory run.

## Experiment Matrix

### A. Prompt Optimization Gate

Run this before any new SFT spending.

Run id proposal:
`20260627-prior-context-count8-prompt-optimizer-g31-optglobal`

Configuration held fixed:

- target model for prompt search: `gemini-3.1-flash-lite`
- target model location: `global`
- optimizer model location: `global`
- context count: `8`
- prior context: same-source transcript block
- decoding: temperature `0.0`, max output tokens `512`, no fallback
- optimizer sample source: train manifests only
- metric: custom `asr_wer_score`
- final prompt selection scorer: full eval manifests, no fallback

Execution:

1. Prepare optimizer data from train rows with target transcripts.
2. Run VAPO P1-P11 with the custom ASR metric.
3. Parse `optimized_results.json`, `templates.json`, and `eval_results.json`
   from the optimizer output.
4. Run the existing no-fallback online scorer for the top prompt packages on
   the full eval set.
5. Select the prompt package by lowest full-eval WER, breaking ties by lower
   empty rate, then lower CER.

Completed gate result:

- Overall VAPO winner: `P9_instruction_and_demo` demonstration stage,
  `asr_wer_score/mean=0.818351`. This prompt includes fixed train-sampled
  few-shot examples, so treat it as a separate few-shot schema rather than the
  default SFT instruction.
- Primary reusable instruction winner: `P3_manual_quality_gate`,
  `asr_wer_score/mean=0.379824`.
- `P11_format_first` failed because VAPO mutated the seed into a non-audio
  task and produced 15/15 failed target predictions. Do not retry it as-is.
- Follow-up user-seeded VAPO winner:
  `P13_user_forensic_context`, `asr_wer_score/mean=0.410378`.
  Artifact:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-user-system-prompts-p3-template-g31-optglobal/outputs/P13_user_forensic_context/instruction/optimized_results.json`.

Primary selected prompt package for SFT:

- system prompt: optimized `P13_user_forensic_context` instruction from the
  follow-up user-seeded VAPO sweep.
- context header: `MANUAL_CONTEXT_HEADER`
- current prompt: `FINAL_AUDIO_CURRENT_PROMPT`
- prompt template shape: same transcript-block shape used by the prompt
  optimizer, with exactly one current audio part.
- rationale: the follow-up VAPO result supports prior context as a constrained
  written-form/spelling reference after the term is already heard, avoids the
  long hard-coded vocabulary inventory, and beats the earlier P3-clean and P6
  candidates on the same custom WER metric.

Acceptance gate:

- The selected prompt package must beat or tie the current seed prompt on
  no-fallback eval WER.
- It must not increase empty responses versus the seed prompt.
- If no optimizer prompt beats the seed, keep the seed prompt and document the
  negative result before SFT.

### B. Confirmatory Optimized-Prompt SFT

Run this after the prompt optimization gate and wait for checkpoint scores
before launching any hyperparameter sweep.

Run id proposal:
`20260627-prior-context-count8-p13-a16-lr075-e10`

Configuration:

- base model: `gemini-3.1-flash-lite`
- continuous tuning: no
- prior context count: `8`
- prior context representation: transcript block using the selected
  `P13_user_forensic_context` prompt package
- system prompt: optimized `P13_user_forensic_context` instruction
- current prompt: `FINAL_AUDIO_CURRENT_PROMPT`
- context header: `MANUAL_CONTEXT_HEADER`
- adapter size: `SIXTEEN`
- learning-rate multiplier: `0.75`
- epoch count: `10`
- validation set: requested four eval manifests
- checkpoint export: all checkpoints retained
- scorer: no fallback, temperature `0.0`, max output tokens `512`,
  safety `OFF`/`BLOCK_NONE` as in the prompt probe

Prediction:

- WER should land below `23.17` because the model is trained on the prompt
  schema selected by prompt optimization and full-eval scoring.
- Empty responses should be close to or below `1.5%`.

Decision:

- If any checkpoint reaches WER `<=22.34` and empty rate `<=0.5%`, accept it as
  the next best model.
- If best WER is `22.34-23.17`, run the LR/adapter refinement.
- If best WER is worse than `23.17`, stop and do error analysis before further
  SFT spending.

Optional follow-up branch:

- Run a separate SFT only if we intentionally want a static few-shot prompt
  package: use the `P9_instruction_and_demo` demonstration prompt as the prompt
  prefix and ensure all demonstrations are train-only.
- Do not mix this into the primary run, because it changes the prompt schema
  from reusable instruction plus dynamic prior context into static
  demonstrations plus dynamic prior context.

### C. Hyperparameter Refinement

Run only if B improves over prompt-only mitigation but does not beat the old
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

### D. Context Robustness

Run only after the transcript-block schema has a successful checkpoint.

| run | context count policy | adapter | LR multiplier | epochs | rationale |
|---|---|---:|---:|---:|---|
| C1 | fixed count `4` | `SIXTEEN` | best from A/B | 8 | test whether count 8 adds avoidable noise |
| C2 | mixed counts `{0, 2, 4, 8}` | `SIXTEEN` | best from A/B | 8 | teach robustness to missing/short histories while preserving count-8 eval |
| C3 | count `0` baseline | `SIXTEEN` | best from A/B | 8 | isolate whether prior context is still adding value after schema fix |

Primary deployment candidate should still be scored with count `8` unless C1 or
C2 clearly wins on full WER and empty rate.

### E. Continuous Tuning

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
