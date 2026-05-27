# Research: Architecture

## Recommended Data Flow

```text
Input manifests/config
  -> manifest loaders
  -> row normalization
  -> source-key inference
  -> hard validation
  -> source-group split optimizer
  -> clip reuse/derivation planner
  -> canonical artifact writer
  -> model-specific writer
  -> report writer
  -> optional upload to GCS
```

## Component Boundaries

### Dataset Registry

Reads a project-local config that lists datasets and input manifests. Each entry should declare:

- dataset name
- dataset family
- manifest URI/path
- parser/source-key strategy
- optional sidecar source map
- whether audio rows are already standalone clips
- model inclusion flags if needed

### Manifest Normalizer

Converts heterogeneous raw rows into one internal labeled-segment shape:

- stable row ID
- dataset name/family
- audio URI
- original audio URI
- offset/duration
- transcript text and normalized text
- source group
- timestamp fields where parseable
- provenance metadata

This should build on `common.manifest.CanonicalRow`, but the splitter needs richer internal metadata than current SFT build rows.

### Source-Key Extractors

Dataset-specific extractors should be pure functions with tests:

- `bcfy_calls:<groupId>` from filename/metadata/URI pattern such as `<unix_ts>-<groupId>`.
- `bcfy_feeds:<feedId>` from archive filename/URL pattern such as `<YYYYMMDDHHMM>-<archive_id>-<feedId>`.
- `echo:<area_code>/<echo_name>` from explicit fields, URL/S3 key, sidecar, or unique CSV mapping.
- `fire_notifications:<stream_path>` from original path/location, not day UUID.

Ambiguity is an exception, not a warning.

### Split Optimizer

The splitter should assign source groups, not rows, to splits. A seeded candidate search can:

1. Shuffle source groups by seed.
2. Build candidate train/eval assignments.
3. Score candidates by weighted imbalance across row count, duration, source count, dataset family, temporal fields, duration buckets, and transcript-length buckets.
4. Select the best candidate that satisfies hard gates.

This matches the validated dry-run result where source-group splitting still achieved about 20% eval rows/duration/sources on current GCS rows.

### Audio Planner

Decides whether each SFT example can reuse an existing clip or requires a derived clip:

- Reuse when the row already points at a standalone supported utterance clip.
- Derive when offset/duration references a longer original audio file.
- Prefer minimal transformation.
- Mix multichannel to mono.
- Avoid padding.
- Avoid resampling unless target-specific generation requires it.
- Fall back to WAV when exact/valid slicing in source format is unreliable.

### Artifact Writers

Canonical writer:

- writes the model-independent train/eval JSONL
- writes per-dataset slices
- includes split/source/provenance/transform metadata

Model writers:

- NeMo writer emits `audio_filepath`, `text`, `duration` JSONL and a config fragment.
- Whisper writer emits a loader-friendly manifest with audio path, text, duration, source metadata, and recommended preprocessing settings.
- Gemini writer emits Vertex SFT JSONL with `systemInstruction`, `contents`, `fileData`, `mimeType`, and target text.

### Reports

Reports should be first-class outputs:

- split summary
- hard leakage report
- balance report
- source-key failure report
- excluded-row report
- transformation/provenance report

## Build Order

1. Source-key extraction and manifest normalization.
2. Leakage validator and deterministic split optimizer.
3. GCS artifact layout/path planner and dry-run reports.
4. Model-specific writers.
5. Audio reuse/derivation execution.
6. End-to-end CLI integration and docs.

## Integration With Existing Code

- Keep shared row/manifest helpers in `model/colabs/common`.
- Keep the CLI and dataset registry under `model/scripts/sft`.
- Reuse existing tests style in `model/colabs/common/tests` and `model/scripts/sft/tests`.
- Do not couple dataset generation to `_tune` or `_eval` stubs.
