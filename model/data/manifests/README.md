# Canonical Manifest Contract

Canonical manifests are row-per-audio-segment JSONL inputs consumed before any
provider-specific model input conversion.

A provided train, validation, or eval manifest must contain at least one row.

## Required Fields

Each JSONL row must include these contract fields:

- `audio_filepath`: stripped, model-ready `gs://...flac` clip URI.
- `text`: non-empty transcript text for the segment.
- `offset`: numeric, non-negative segment offset.
- `duration`: numeric segment duration; must be positive.
- `example_id`: logical example identifier.
- `segment_id`: logical segment identifier.

`(example_id, segment_id)` must be unique within one manifest. Strict
validation also rejects duplicate `audio_filepath` values within one manifest.

## Optional Metadata

Strict validation accepts these optional metadata fields:

- `split`: split label, such as `train`, `validation`, or `eval`.
- `dataset.name`: dataset name, such as `echo`.
- `dataset.family`: dataset family, such as `radio`.
- `source_audio.audio_filepath`: source audio locator.
- `source_audio.offset`: numeric, non-negative segment offset in the source
  audio.
- `source_audio.duration`: numeric source segment duration; must be positive.

Unknown row-level fields, unknown keys inside optional metadata blocks, and
prediction-enriched fields such as `pred_text_*` are tolerated by strict
validation. Conversion to `CanonicalRow` preserves unknown keys inside
`dataset` and `source_audio` while normalizing the known contract keys.

## JSONL Example

```jsonl
{"audio_filepath":"gs://watch-duty-model-ready/train/example-001-seg-001.flac","text":"Engine 42 responding to the incident.","offset":0.0,"duration":4.25,"example_id":"example-001","segment_id":"seg-001","split":"train","dataset":{"name":"echo","family":"radio"},"source_audio":{"audio_filepath":"gs://watch-duty-raw/source/example-001.wav","offset":128.5,"duration":4.25}}
```

## Helper Entry Points

- `validate_canonical_manifest(...)` returns structured issues for strict
  contract violations.
- `require_canonical_manifest(...)` calls the validator and raises one
  aggregated `ValueError` when issues are present.
- `canonical_row_identity(...)` returns `(example_id, segment_id)` for a
  canonical row.
- `load_manifest()` loads local JSON arrays or JSONL files and fails loudly on
  missing or malformed files. It does not coerce row values.
- `rows_from_manifest()` is a compatibility converter: it derives a missing
  `example_id` from the audio filename, defaults a missing `segment_id` to
  `"001"` and a missing `offset` to `0.0`, then validates and returns typed
  `CanonicalRow` instances. Call `require_canonical_manifest(...)` first when
  raw input must satisfy the strict contract without compatibility defaults.

For documentation-only edits, use lightweight checks such as
`git diff --check` on the changed files.
