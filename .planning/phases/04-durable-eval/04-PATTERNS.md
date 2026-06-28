# Phase 4: Durable Eval - Pattern Map

## Pattern Mapping Complete

### Files To Modify

| File | Role | Closest Existing Analog |
|------|------|-------------------------|
| `model/src/gemini_sft/config.py` | Local TOML and durable config contract | Existing `EvalModelTarget`, `require_config_eval_models`, and `EvalExecutionConfig` parsing |
| `model/src/gemini_sft/evaluate.py` | One-model eval orchestration | Current target loop plus Phase 3 backend dispatch |
| `model/src/gemini_sft/target_execution.py` | Online identity and backend resolver | Existing `build_online_request_identity` and metadata validation |
| `model/src/common/gemini/batch.py` | Batch input/output and reuse | Current `run_batch_audio_inference` path-based reuse |
| `model/src/gemini_sft/records.py` | Local summary rendering and ledger | Existing `write_wer_summary` local writer |
| `model/scripts/sft/score_gemini_sft_checkpoints_online.py` | Legacy checkpoint sweep compatibility | Current package online executor delegation |
| `model/scripts/sft/run_config.example.toml` | Operator example | Current plural eval target example |
| `model/scripts/sft/README.md` | Operator docs | Current eval config and artifact sections |

### Patterns To Preserve

- Config parsing raises `RunConfigError` for local TOML and `ValueError` or
  `TypeError` for invalid durable `config.json` state.
- Safe artifact labels are validated by `common.inference_manifest.validate_artifact_label`.
- Durable eval execution reads from GCS `config.json`, not from local TOML
  values that can drift after prepare/tune.
- Online prediction reuse validates metadata before paid calls. Missing
  metadata for existing predictions is an error, not a signal to resubmit.
- Missing provider rows remain empty hypotheses for scoring, while normalized
  inference manifests omit `pred_text_*` only when a provider row is missing.
- Report JSON, Markdown, and console all flow through `EvalReport` and
  `TargetMetrics`.
- Tests use `FakeStorageClient` and patches around Vertex boundaries.

### Concrete Existing Snippets

Config target serialization currently uses:

```python
def to_record_dict(self) -> dict[str, str]:
    return {"label": self.label, "model": self.model}
```

Online identity includes:

```python
"target_label": target_label,
"model": model,
"eval_manifest_uri": eval_manifest_uri,
"audio_uris": list(audio_uris),
"system_prompt": system_prompt,
"user_prompt": user_prompt,
"prior_context_count": prior_context_count,
"prior_context_mode": prior_context_mode,
"generation_config": _json_safe_copy(generation_config),
"safety_settings": _json_safe_copy(list(safety_settings)),
```

Batch output paths currently use:

```python
batch_input_gcs = f"{run_gcs_prefix}/evals/{label}/input.jsonl"
batch_output_gcs = f"{run_gcs_prefix}/evals/{label}/output/"
```

Summary artifact fields already exist:

```python
summary_json_uri: str | None = None
summary_markdown_uri: str | None = None
```

### Implementation Boundaries

- Do not add dataset breakdown code in Phase 4.
- Do not add internal multi-model or multi-target parallelism.
- Do not keep plural target config as a compatibility path.
- Do not make local `results/` authoritative for reuse or report links.
- Do not import `gemini_sft` package code from `common.gemini` modules.
