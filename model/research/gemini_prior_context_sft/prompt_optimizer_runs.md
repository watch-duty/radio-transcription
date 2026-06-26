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
- status: pending/provisioning at submission

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

Next step: wait for the second fixed smoke run. If it succeeds, relaunch the
full prompt-family sweep with `target_model_location=us` and
`custom_metric_cloud_function_location=us-central1`, parse optimized prompts,
then score the top prompt packages on the full four-dataset eval set with no
fallback before selecting the prompt for SFT.
