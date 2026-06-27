# Prompt Optimizer Runs

## 2026-06-26: Custom Metric

Deployed HTTP custom metric function:

- function: `projects/automatic-hawk-481415-m9/locations/us-central1/functions/wd-asr-wer-score`
- Cloud Run service URI: `https://wd-asr-wer-score-lu3a6psyna-uc.a.run.app`
- metric key: `asr_wer_score`
- invoker granted to: `781667204380-compute@developer.gserviceaccount.com`

Smoke call through `gcloud functions call` returned:

```json
{"asr_wer_score":1.0,"explanation":"normalized_wer=0.0000; empty_response=False"}
```

## 2026-06-26: Smoke VAPO Job

Initial smoke run:

- output prefix:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-prior-context-count8-prompt-optimizer-smoke`
- family: `P1_force_words_incumbent`
- sample rows: `24`
- data limit: `24`
- job:
  `projects/781667204380/locations/us-central1/customJobs/8481810826137698304`
- result: failed
- root cause: the VAPO config used `target_model_location=us-central1`.
  Direct `google-genai` validation showed `gemini-3.1-flash-lite` is callable
  from Vertex location `us` in this project, while `us-central1` returns
  publisher-model `404`.

Fixed smoke run:

- output prefix:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-prior-context-count8-prompt-optimizer-smoke-us`
- family: `P1_force_words_incumbent`
- sample rows: `24`
- data limit: `24`
- VAPO steps: `2`
- optimizer job location: `us-central1`
- target model location: `us`
- job:
  `projects/781667204380/locations/us-central1/customJobs/4583945348648534016`
- result: failed
- root cause: once `target_model_location=us` was set, VAPO inferred the
  custom metric function location as `us` and validated
  `https://us-automatic-hawk-481415-m9.cloudfunctions.net/wd-asr-wer-score`.
  The deployed function is in `us-central1`, so the validation returned `404`.

Second fixed smoke run:

- output prefix:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-prior-context-count8-prompt-optimizer-smoke-us2`
- family: `P1_force_words_incumbent`
- sample rows: `24`
- data limit: `24`
- VAPO steps: `2`
- optimizer job location: `us-central1`
- target model location: `us`
- custom metric function location: `us-central1`
- job:
  `projects/781667204380/locations/us-central1/customJobs/8889949542368149504`
- result: failed
- root cause: the custom metric location was fixed and validated, but VAPO's
  target-model calls to `gemini-3.1-flash-lite` in `us` returned HTTP `400`.
  Direct `google-genai` audio calls to the same model, location, sample audio,
  decoding params, and safety thresholds succeeded, so this appears specific to
  the VAPO worker path for this target model.

Compatibility smoke runs:

| target model | target location | job |
|---|---|---|
| `gemini-3.5-flash` | `us` | `projects/781667204380/locations/us-central1/customJobs/7801626544916398080` |
| `gemini-2.5-flash-lite` | `us-central1` | `projects/781667204380/locations/us-central1/customJobs/1445921570789785600` |

Result: both compatibility jobs were cancelled before optimizer execution after
review because prompt optimization must run only against
`gemini-3.1-flash-lite`.

Third 3.1-only smoke run:

- output prefix:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-prior-context-count8-prompt-optimizer-smoke-g31global`
- family: `P1_force_words_incumbent`
- sample rows: `24`
- data limit: `24`
- VAPO steps: `2`
- optimizer job location: `us-central1`
- target model: `gemini-3.1-flash-lite`
- target model location: `global`
- custom metric function location: `us-central1`
- job:
  `projects/781667204380/locations/us-central1/customJobs/330154763108745216`
- result: failed
- root cause: VAPO called the legacy `v1beta1` global prediction endpoint
  (`https://global-aiplatform.googleapis.com/v1beta1/...`) for
  `gemini-3.1-flash-lite`, which returned publisher-model `404`. Direct local
  checks with the same model succeeded through `google-genai` in both `us` and
  `global`, and through the legacy Vertex SDK in `global`, so this is specific
  to VAPO's worker routing path.

Fourth 3.1-only smoke run:

- output prefix:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-prior-context-count8-prompt-optimizer-smoke-g31global-fullname`
- family: `P1_force_words_incumbent`
- sample rows: `24`
- data limit: `24`
- VAPO steps: `2`
- optimizer job location: `us-central1`
- target model:
  `projects/automatic-hawk-481415-m9/locations/global/publishers/google/models/gemini-3.1-flash-lite`
- target model location: `global`
- custom metric function location: `us-central1`
- job:
  `projects/781667204380/locations/us-central1/customJobs/2043774421323218944`
- result: failed
- root cause: passing the full model resource did not change VAPO's target
  inference route. The worker still called the legacy `v1beta1` global
  endpoint and received per-example publisher-model `404` errors:
  `POST https://global-aiplatform.googleapis.com/v1beta1/projects/automatic-hawk-481415-m9/locations/global/publishers/google/models/gemini-3.1-flash-lite:generateContent`.

## 2026-06-26: Full Prompt-Family VAPO Jobs

Initial full run:

- output prefix:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-prior-context-count8-prompt-optimizer`
- sample rows: `600`
- VAPO `data_limit`: `100`
- metric: custom `asr_wer_score`

| family | custom job |
|---|---|
| `P1_force_words_incumbent` | `projects/781667204380/locations/us-central1/customJobs/3897146405474533376` |
| `P2_canonical_gemini` | `projects/781667204380/locations/us-central1/customJobs/3852110409200828416` |
| `P3_manual_quality_gate` | `projects/781667204380/locations/us-central1/customJobs/8094501258183835648` |
| `P4_chirp_phrase_hints` | `projects/781667204380/locations/us-central1/customJobs/3084246672734158848` |
| `P5_compact_common_asr` | `projects/781667204380/locations/us-central1/customJobs/769396464265723904` |
| `P6_output_only` | `projects/781667204380/locations/us-central1/customJobs/1951591366450479104` |
| `P7_error_aware_context` | `projects/781667204380/locations/us-central1/customJobs/3307174854288998400` |
| `P8_no_empty_unintelligible` | `projects/781667204380/locations/us-central1/customJobs/8180069651103875072` |
| `P9_instruction_and_demo` | `projects/781667204380/locations/us-central1/customJobs/3345455451121647616` |
| `P10_canary_minimal` | `projects/781667204380/locations/us-central1/customJobs/3491822439011188736` |
| `P11_format_first` | `projects/781667204380/locations/us-central1/customJobs/8972140235567661056` |

Result: all failed for the same `target_model_location=us-central1` issue.

Conclusion: Vertex AI Prompt Optimizer is blocked for
`gemini-3.1-flash-lite` in this project. Direct `google-genai` validation
against `gemini-3.1-flash-lite` succeeds in `us` and `global`, but the VAPO
worker uses a legacy `v1beta1` path that either 404s at `global` or returns
per-example target failures at `us`.

Next step: run a direct `google-genai` prompt-family gate against
`gemini-3.1-flash-lite` only. This is a fallback for prompt selection, not a
Vertex Prompt Optimizer result; it keeps the no-fallback rule and the same
prior-conversation request shape planned for SFT.

## 2026-06-26: Gemini 3 Non-Pro Compatibility Smokes

After review, expanded compatibility checks beyond `gemini-3.1-flash-lite` to
other Gemini 3 non-Pro targets only.

Live Vertex model catalog candidates for ASR-style targets:

- `gemini-3-flash-preview`
- `gemini-3.1-flash-lite-preview`
- `gemini-3.5-flash`

Excluded `gemini-3.1-flash-image`, `gemini-3.1-flash-image-preview`, and
`gemini-3.1-flash-tts-preview` because they are not appropriate ASR
transcription targets for this prompt/SFT gate.

Direct one-audio `google-genai` smoke:

| target model | location | result |
|---|---|---|
| `gemini-3-flash-preview` | `us` | publisher-model `404` |
| `gemini-3-flash-preview` | `global` | callable, empty transcript on smoke row |
| `gemini-3-flash-preview` | `us-central1` | publisher-model `404` |
| `gemini-3.1-flash-lite-preview` | `global` | callable, non-empty transcript |
| `gemini-3.1-flash-lite-preview` | `us-central1` | publisher-model `404` |
| `gemini-3.5-flash` | `us` | callable, empty transcript on smoke row |
| `gemini-3.5-flash` | `global` | callable, empty transcript on smoke row |
| `gemini-3.5-flash` | `us-central1` | publisher-model `404` |

Submitted VAPO smoke jobs:

| target model | target location | job | status |
|---|---|---|---|
| `gemini-3-flash-preview` | `global` | `projects/781667204380/locations/us-central1/customJobs/4676691353474695168` | failed |
| `gemini-3.1-flash-lite-preview` | `global` | `projects/781667204380/locations/us-central1/customJobs/7366466230921723904` | failed |
| `gemini-3.5-flash` | `us` | `projects/781667204380/locations/us-central1/customJobs/7497070620115468288` | failed |

First five-minute poll: all three jobs were still `JOB_STATE_RUNNING`.

Failure causes:

- `gemini-3-flash-preview` and `gemini-3.1-flash-lite-preview` failed for the
  same global VAPO routing issue as `gemini-3.1-flash-lite`: the VAPO worker
  called `global-aiplatform.googleapis.com/v1beta1/...:generateContent` and
  received publisher-model `404` for every example.
- `gemini-3.5-flash` at `us` reached
  `https://us-aiplatform.googleapis.com/v1beta1/.../gemini-3.5-flash:generateContent`
  but every target call returned HTTP `400 Bad Request`.

Submitted one additional `gemini-3.5-flash` retry with
`target_model_harm_block_threshold=OFF` to test whether the `400` came from
VAPO's default `BLOCK_LOW_AND_ABOVE` safety setting:

- job:
  `projects/781667204380/locations/us-central1/customJobs/547565380567040`
- result: failed
- conclusion: VAPO parsed `target_model_harm_block_threshold=OFF` and used it
  in `VertexGenerateFn`, so the `gemini-3.5-flash` failure is not explained by
  the target safety threshold.

## 2026-06-26: PR 800 VAPO Setup

Reviewed https://github.com/watch-duty/radio-transcription/pull/800. The PR
adds `model/colabs/optimize_transcription_prompt.ipynb`, an interactive Colab
for VAPO prompt tuning.

Final notebook setup:

- installs `google-genai>=2.3,<3`, `google-cloud-storage`,
  `pydantic<=2.12.3`, and `tqdm`
- imports `genai.Client` plus `types.PromptOptimizerMethod.VAPO`
- reads Colab secrets `GCP_PROJECT_ID`, `GCS_BUCKET`, and
  `GCP_PROJECT_NUMBER`
- defaults to project `automatic-hawk-481415-m9`, bucket
  `wd-transcription-data`, job location `us-central1`
- default input directory:
  `broadcastify/calls/eval_audio_masked_v2`
- compiles a segmented-audio `batch_manifest.jsonl` into VAPO rows:
  `{"input_text": audio_filepath, "target": text}`
- uses `common.manifest.is_scoreable_manifest_entry` for row eligibility
- writes the compiled dataset to:
  `gs://<bucket>/segmented_audio/<INPUT_AUDIO_DIR>/apo_dataset.jsonl`
- configures VAPO with:
  - `system_instruction`: `GEMINI_TRANSCRIBE_SYSTEM_PROMPT` unless overridden
  - `prompt_template`: `{input_text} @@@audio/flac\n{target}`
  - `target_model`: `gemini-3.1-flash-lite`
  - `thinking_budget`: `0`
  - `optimization_mode`: `instruction_and_demo`
  - `eval_metrics_types`: `["bleu", "rouge_l"]`
  - `eval_metrics_weights`: `[0.5, 0.5]`
  - `num_steps`: `10`
  - `num_demo_set_candidates`: `10`
  - `demo_set_size`: `3`
  - `target_model_location`: `global`
  - `optimizer_model_location`: `global`
  - `has_multimodal_inputs`: `True`
  - `data_limit`: `50`
- launches with service account:
  `<GCP_PROJECT_NUMBER>-compute@developer.gserviceaccount.com`
- includes helper cells for listing/describing/canceling custom jobs
- includes a Gradio viewer that expects VAPO outputs containing
  `templates.json`, `eval_results.json`, and `optimized_results.json`

Found one successful historical job from this setup lineage:

- job:
  `projects/781667204380/locations/us-central1/customJobs/1690325387688542208`
- display name: `vapo-optimizer-20260619-093453`
- state: `JOB_STATE_SUCCEEDED`
- config:
  `gs://wd-transcription-data/segmented_audio/broadcastify/calls/vapo_outputs/config.json`
- input data:
  `gs://wd-transcription-data/segmented_audio/broadcastify/calls/training_audio_masked_v2/apo_dataset.jsonl`
- output:
  `gs://wd-transcription-data/segmented_audio/broadcastify/calls/vapo_outputs/results/`

Important differences from the prior-context prompt-optimizer gate:

- PR 800 used a plain audio-only dataset with only `input_text` and `target`.
  It did not include transcript prior context.
- PR 800 used VAPO's built-in BLEU/ROUGE composite metric, not the custom WER
  Cloud Function metric.
- The successful historical config used `prompt_template="{input_text}\n{target}"`
  and `has_multimodal_inputs=True`; its dataset rows contained bare GCS FLAC
  URIs, not `@@@audio/flac` markers. The final PR notebook later added the
  marker based on review feedback.
- The successful historical nested output config shows VAPO resolved
  `target_model_harm_block_threshold="OFF"` and
  `optimizer_model_harm_block_threshold="OFF"`.
- The successful run's `error.json` contains a stale/earlier validation error
  for rows using `output_text` instead of `target`, but the job itself
  ultimately succeeded after the dataset was rewritten with `target`.

## 2026-06-26: PR 800 Isolation Smokes

Added launcher support for:

- `optimizer_model_location`
- `target_model_harm_block_threshold`
- PR800-style audio-only sample rows:
  `{"input_text": audio_filepath, "target": text}`
- PR800-style prompt templates:
  - plain: `{input_text}\n{target}`
  - marker: `{input_text} @@@audio/flac\n{target}`
- BLEU/ROUGE composite metric:
  `eval_metrics_types=["bleu", "rouge_l"]`,
  `eval_metrics_weights=[0.5, 0.5]`

Submitted four isolation jobs against `gemini-3.1-flash-lite`:

| experiment | dataset shape | metric | target location | optimizer location | job | status |
|---|---|---|---|---|---|---|
| PR800 plain | `pr800_plain` | BLEU/ROUGE | `global` | `global` | `projects/781667204380/locations/us-central1/customJobs/7109479577184894976` | pending at submission |
| PR800 marker | `pr800_marker` | BLEU/ROUGE | `global` | `global` | `projects/781667204380/locations/us-central1/customJobs/7700858503253983232` | pending at submission |
| prior context custom | `prior_context` | custom WER | `global` | `global` | `projects/781667204380/locations/us-central1/customJobs/4745089772815384576` | pending at submission |
| prior context BLEU/ROUGE | `prior_context` | BLEU/ROUGE | `global` | `global` | `projects/781667204380/locations/us-central1/customJobs/2900865725407166464` | failed validation |

Submitted one additional closer PR800 reproduction:

| experiment | dataset shape | metric | optimization mode | rows | job | status |
|---|---|---|---|---:|---|---|
| PR800 plain demo | `pr800_plain` | BLEU/ROUGE | `instruction_and_demo` | 50 | `projects/781667204380/locations/us-central1/customJobs/6847144898890563584` | pending at submission |

The first prior-context BLEU/ROUGE job failed before target-model inference:
VAPO's built-in metrics require a prompt-template placeholder named `target`.
Submitted a corrected built-in-metric prior-context job with `{target}` appended
to the prompt template:

- job:
  `projects/781667204380/locations/us-central1/customJobs/7531692042250878976`
- status: pending at submission

First isolation poll:

- `pr800_plain` BLEU/ROUGE:
  `projects/781667204380/locations/us-central1/customJobs/7109479577184894976`
  succeeded.
- `pr800_marker` BLEU/ROUGE:
  `projects/781667204380/locations/us-central1/customJobs/7700858503253983232`
  succeeded.
- `prior_context` custom WER:
  `projects/781667204380/locations/us-central1/customJobs/4745089772815384576`
  succeeded.
- `pr800_plain` `instruction_and_demo`:
  `projects/781667204380/locations/us-central1/customJobs/6847144898890563584`
  still running.
- corrected `prior_context` BLEU/ROUGE:
  `projects/781667204380/locations/us-central1/customJobs/7531692042250878976`
  still running.

The successful prior-context custom WER smoke produced
`outputs/P2_canonical_gemini/instruction/optimized_results.json` with
`asr_wer_score/mean=0.1961`. Its nested config confirmed the working VAPO
setup:

- `target_model_location="global"`
- `optimizer_model_location="global"`
- `target_model_harm_block_threshold="OFF"`
- `eval_metric="custom_metric"`
- `custom_metric_cloud_function_location="us-central1"`

Final isolation status:

- `pr800_plain` BLEU/ROUGE succeeded.
- `pr800_marker` BLEU/ROUGE succeeded.
- `pr800_plain` `instruction_and_demo` succeeded.
- corrected `prior_context` BLEU/ROUGE succeeded.
- `prior_context` custom WER succeeded.
- the only isolation failure was the expected validation failure for the
  built-in-metric prior-context template that omitted a `target` placeholder.

Conclusion: the current VAPO backend can run `gemini-3.1-flash-lite` with
multimodal audio, prior transcript context, and the custom WER Cloud Function.
The required settings are `target_model_location="global"`,
`optimizer_model_location="global"`, and
`target_model_harm_block_threshold="OFF"`.

## 2026-06-26: Full Prompt-Family Sweep Relaunch

Since the prior-context custom WER smoke succeeded, relaunched the full
prompt-family sweep with the working setup:

- output prefix:
  `gs://wd-transcription-data/sft/experiments/gemini-prior-context-sft-v20260625/prompt_optimizer/20260627-prior-context-count8-prompt-optimizer-g31-optglobal`
- sample rows: `600`
- VAPO `data_limit`: `100`
- metric: custom `asr_wer_score`
- target model: `gemini-3.1-flash-lite`
- target model location: `global`
- optimizer model location: `global`
- target model harm block threshold: `OFF`
- custom metric function location: `us-central1`

| family | custom job |
|---|---|
| `P1_force_words_incumbent` | `projects/781667204380/locations/us-central1/customJobs/6292639194770571264` |
| `P2_canonical_gemini` | `projects/781667204380/locations/us-central1/customJobs/2269235877668454400` |
| `P3_manual_quality_gate` | `projects/781667204380/locations/us-central1/customJobs/5106503642911866880` |
| `P4_chirp_phrase_hints` | `projects/781667204380/locations/us-central1/customJobs/1377523151449096192` |
| `P5_compact_common_asr` | `projects/781667204380/locations/us-central1/customJobs/606281715261898752` |
| `P6_output_only` | `projects/781667204380/locations/us-central1/customJobs/2897488025686638592` |
| `P7_error_aware_context` | `projects/781667204380/locations/us-central1/customJobs/5171805837508739072` |
| `P8_no_empty_unintelligible` | `projects/781667204380/locations/us-central1/customJobs/7815418818775220224` |
| `P9_instruction_and_demo` | `projects/781667204380/locations/us-central1/customJobs/3928812340354482176` |
| `P10_canary_minimal` | `projects/781667204380/locations/us-central1/customJobs/8927244976782311424` |
| `P11_format_first` | `projects/781667204380/locations/us-central1/customJobs/3294930692802084864` |

First five-minute full-sweep poll: all 11 jobs were `JOB_STATE_RUNNING`.

Final status:

- `P1_force_words_incumbent`: succeeded
- `P2_canonical_gemini`: succeeded
- `P3_manual_quality_gate`: succeeded
- `P4_chirp_phrase_hints`: succeeded
- `P5_compact_common_asr`: succeeded
- `P6_output_only`: succeeded
- `P7_error_aware_context`: succeeded
- `P8_no_empty_unintelligible`: succeeded
- `P9_instruction_and_demo`: succeeded
- `P10_canary_minimal`: succeeded
- `P11_format_first`: failed

Final optimizer-sample ranking:

| rank | family | stage | `asr_wer_score/mean` | step | prompt chars |
|---:|---|---|---:|---:|---:|
| 1 | `P9_instruction_and_demo` | `instruction/demonstration` | 0.818351 | 10 | 3201 |
| 2 | `P3_manual_quality_gate` | `instruction` | 0.379824 | 7 | 8024 |
| 3 | `P1_force_words_incumbent` | `instruction` | 0.362314 | 10 | 2750 |
| 4 | `P6_output_only` | `instruction` | 0.357437 | 10 | 4563 |
| 5 | `P8_no_empty_unintelligible` | `instruction` | 0.345953 | 9 | 4383 |
| 6 | `P9_instruction_and_demo` | `instruction` | 0.317367 | 2 | 1187 |
| 7 | `P2_canonical_gemini` | `instruction` | 0.313137 | 10 | 6419 |
| 8 | `P5_compact_common_asr` | `instruction` | 0.307074 | 9 | 4140 |
| 9 | `P4_chirp_phrase_hints` | `instruction` | 0.304320 | 4 | 4996 |
| 10 | `P7_error_aware_context` | `instruction` | 0.303005 | 5 | 2985 |
| 11 | `P10_canary_minimal` | `instruction` | 0.300115 | 4 | 2261 |

Selection interpretation:

- Overall VAPO winner is `P9_instruction_and_demo` in
  `instruction/demonstration` mode. This prompt includes fixed train-sampled
  few-shot examples in the prompt body, so it is a different schema from a pure
  reusable instruction prompt.
- Best reusable instruction-only winner is `P3_manual_quality_gate` with
  `asr_wer_score/mean=0.379824`.
- Use `P3_manual_quality_gate` as the primary next SFT prompt. Treat P9
  demonstration mode as an optional follow-up branch only if we intentionally
  want static train-only few-shot examples in every SFT/eval request.

`P11_format_first` failure diagnosis:

- The job used the same working VAPO infrastructure settings as the successful
  jobs: `target_model_location="global"`,
  `optimizer_model_location="global"`,
  `target_model_harm_block_threshold="OFF"`, and custom WER metric in
  `us-central1`.
- The failure was prompt-family-specific. The format-first seed let VAPO mutate
  the instruction into a non-audio task, including candidates that said the
  model was not an audio-to-text transcriber and should ignore audio file paths.
- The terminal error was:

```text
No successful per-example results found (15 failures).
```

This happened after a candidate produced 15/15 failed target predictions, with
`nan` entering the UCB stats. Treat `P11` as an invalid family, not as a missing
infrastructure result. Do not reschedule it for the current sweep. If a
formatting-focused family is still desired later, create a new `P11b` that
hard-pins the task as audio transcription and only optimizes a small formatting
clause such as one-line output and digit formatting.
