# Canonical Manifest Contract

Canonical manifests are row-per-audio-segment JSONL inputs consumed before any
provider-specific model input conversion.

A provided train, validation, or eval manifest must contain at least one row.

## Required Fields

Each JSONL row must include these contract fields:

- `audio_filepath`: stripped, model-ready `gs://...flac` clip URI.
- `text`: non-empty transcript text for the segment.
- `offset`: numeric segment offset.
- `duration`: numeric segment duration; must be positive.
- `example_id`: logical example identifier.
- `segment_id`: logical segment identifier.

`(example_id, segment_id)` is the logical row identity and must be unique
within one manifest. Strict validation also rejects duplicate
`audio_filepath` values within one manifest.

## Optional Metadata

Strict validation accepts these optional metadata fields:

- `split`: split label, such as `train`, `validation`, or `eval`.
- `dataset.name`: dataset name, such as `echo`.
- `dataset.family`: dataset family, such as `radio`.
- `source_audio.audio_filepath`: source audio locator.
- `source_audio.offset`: numeric segment offset in the source audio.
- `source_audio.duration`: numeric source segment duration; must be positive.

Unknown row-level fields, unknown keys inside optional metadata blocks, and
prediction-enriched fields such as `pred_text_*` are tolerated by strict
validation.

## JSONL Example

```jsonl
{"audio_filepath":"gs://watch-duty-model-ready/train/example-001-seg-001.flac","text":"Engine 42 responding to the incident.","offset":0.0,"duration":4.25,"example_id":"example-001","segment_id":"seg-001","split":"train","dataset":{"name":"echo","family":"radio"},"source_audio":{"audio_filepath":"gs://watch-duty-raw/source/example-001.wav","offset":128.5,"duration":4.25}}
```

## Adapter Outputs

Downstream tools should derive their provider-specific inputs from the
canonical row:

- AdalFlow and DSPy prompt tuning use `audio_filepath`, `text`, and
  `duration` as prompt-example context.
- Vertex AI Prompt Optimizer uses `{"input_text": audio_filepath, "target":
  text}`.
- Vertex AI batch inference uses `audio_filepath` in Gemini request JSONL.
- Vertex AI SFT uses `audio_filepath` and `text` in Gemini tuning JSONL.
- Agent-session evaluation groups rows by `example_id` and sorts by
  `source_audio.offset` when source ordering matters.

## Helper Entry Points

- `validate_canonical_manifest(...)` returns structured issues for strict
  contract violations.
- `require_canonical_manifest(...)` calls the validator and raises one
  aggregated `ValueError` when issues are present.
- `canonical_row_identity(...)` returns `(example_id, segment_id)` for a
  canonical row.
- `load_manifest()` loads local JSON arrays or JSONL files and fails loudly on
  missing or malformed files. It does not coerce row values.
- `rows_from_manifest()` validates canonical rows and converts them to
  typed `CanonicalRow` instances.

For documentation-only edits, use lightweight checks such as
`git diff --check` on the changed files.
