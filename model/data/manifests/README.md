# Canonical Manifest Contract

Canonical manifests are row-per-audio-segment JSONL inputs consumed before any
provider-specific model input conversion. New strict train/eval manifests use
one row shape and should not carry duplicate legacy lineage fields.

A provided train, validation, or eval manifest must contain at least one row.

## Required Fields

Each JSONL row must include exactly these core contract fields:

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

Strict validation accepts shallow optional metadata without requiring every
generator to emit it:

- `split`: optional split label used by callers that validate an expected
  train, validation, or eval split.
- `lang`: optional language label.
- `dataset.name` and `dataset.family`: optional non-empty strings when
  `dataset` is present.
- `source_audio.audio_filepath`: optional source-audio locator when
  `source_audio` is present.
- `source_audio.offset` and `source_audio.duration`: optional numeric source
  timing metadata; source duration must be positive.
- `audio_processing.masked_categories`: optional list of non-empty category
  names when `audio_processing` is present.

Unknown row-level fields, unknown keys inside optional metadata blocks, and
prediction-enriched fields such as `pred_text_*` are tolerated by strict
validation.

## JSONL Example

```jsonl
{"audio_filepath":"gs://watch-duty-model-ready/train/example-001-seg-001.flac","text":"Engine 42 responding to the incident.","offset":0.0,"duration":4.25,"example_id":"example-001","segment_id":"seg-001","split":"train","lang":"en","dataset":{"name":"watch-duty-radio","family":"dispatch"},"source_audio":{"audio_filepath":"gs://watch-duty-raw/source/example-001.wav","offset":128.5,"duration":4.25},"audio_processing":{"masked_categories":["phone_number"]},"pred_text_baseline":"Engine 42 responding to the incident."}
```

## Helper Entry Points

- `validate_canonical_manifest(...)` returns structured issues for strict
  contract violations.
- `require_canonical_manifest(...)` calls the validator and raises one
  aggregated `ValueError` when issues are present.
- `canonical_row_identity(...)` returns `(example_id, segment_id)` for a
  canonical row.
- `load_manifest()` remains a lenient JSON/JSONL parser for exploratory
  loading.
- `rows_from_manifest()` is the compatibility conversion to typed rows and
  fails loudly when `audio_filepath` or `text` is missing or blank.

For documentation-only edits, use lightweight checks such as
`git diff --check` on the changed files.

## Deprecated Duplicate Fields

New canonical rows should not emit duplicate lineage or denormalized fields
that were used by older manifests, including:

- `audio_uri`
- `model_ready_audio_uri`
- `derived_audio_uri`
- `original_audio_uri`
- `original_offset`
- `dataset_name`
- `dataset_family`
- `source_group`
- `source_strategy`
- `transformation_metadata`
- `timestamp`
- top-level `category`
