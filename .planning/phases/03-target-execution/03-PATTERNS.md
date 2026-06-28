# Phase 3 Patterns: Target Execution

**Phase:** 03-target-execution
**Date:** 2026-06-28
**Status:** Complete

## Existing Patterns To Preserve

### Config Parsing

- External TOML validation lives in `gemini_sft.config`.
- Invalid local TOML raises `RunConfigError`.
- Invalid durable `config.json` raises `ValueError` or `TypeError` through
  small `require_config_*` helpers.
- Config validation is static. Do not call GCS or Vertex from config parsing.
- Optional config tables should reject unsupported fields instead of silently
  accepting a larger API surface.

### Durable Run State

- `RunConfig.to_record_dict()` writes the durable `config.json` shape under
  `gs://<bucket>/sft/runs/<round-id>/config.json`.
- `gemini-sft eval` loads authoritative config from GCS before provider work.
- Local `results/<round-id>/` remains a mirror/cache.

### Request And Context Construction

- `common.gemini.vertex.build_request(...)` is the only request-construction
  path for maintained batch and online inference.
- `common.gemini.context.build_context_histories(...)` is the only dynamic
  prior-context builder.
- Generation and safety settings come from
  `GEMINI_GENERATION_CONFIG` and `GEMINI_SAFETY_SETTINGS`.

### Batch Target Execution

Use `common.gemini.batch.run_batch_audio_inference` for batch targets. It
already owns input JSONL writing, output reuse, batch submission, parsing,
duplicate audio URI rejection, and missing-prediction warnings.

### Online Target Execution

Extract script-local online mechanics from
`model/scripts/sft/score_gemini_sft_checkpoints_online.py` into package code:

- endpoint location extraction
- async generate-content calls
- retry attempts
- bounded concurrency
- local append and periodic GCS sync
- resume from existing prediction JSONL
- empty prediction plus `error` field for exhausted rows

### Reporting

- Use `gemini_sft.reporting.build_target_metrics` for every target.
- Keep report columns unchanged from Phase 1.
- Use target metadata for backend, request identity, and online error count.
- Keep missing predictions in the denominator by building hypotheses in eval
  row order.

## New Phase 3 Patterns

### Execution Config

Represent `[eval.execution]` with a small frozen dataclass:

```python
@dataclass(frozen=True)
class EvalExecutionConfig:
    backend: str | None = None
    limit: int | None = None
    concurrency: int = 16
    max_retries: int = 3
```

Allowed backend values are only `"batch"` and `"online"`. Omitted backend means
default routing, not an `"auto"` value.

### Backend Resolver

Keep target records unchanged. Resolve execution in code from the target model
string plus optional config-wide override:

```text
forced backend present -> forced backend
model contains "/endpoints/" -> online
otherwise -> batch
```

This preserves Phase 2's `[[eval.models]]` shape and keeps endpoint handling
conservative.

### Online Request Identity

Store online request identity beside predictions:

```text
<run_gcs_prefix>/evals/<target-label>/online_predictions.jsonl
<run_gcs_prefix>/evals/<target-label>/online_predictions.meta.json
```

The sidecar should contain a stable hash and the canonical identity fields:
target label, target model, eval manifest URI, evaluated audio URI order,
system prompt, user prompt, prior context count, prior context mode,
generation config, and safety settings.

Operational settings such as concurrency and retry count should not be part of
the request identity. They can be recorded in metadata for debugging.

### Smoke Prefix Reuse

`limit` truncates `source_rows`, `eval_rows`, `audio_uris`, and histories after
canonical eval parsing. A smoke run may be reused by a later full run only when
the previous audio URI list is a prefix of the current URI list and the rest of
the identity matches exactly.

### Target Execution Result

Use a small package-owned result object or prediction map that carries:

- `backend`
- `predictions_by_audio_uri`
- raw batch output URI or online prediction URI
- online metadata URI when applicable
- `error_count`

`gemini_sft.evaluate` can turn this into `ReportArtifacts` and
`TargetMetrics`.

