# Phase 1: Manifest And Source Identity - Pattern Map

## Closest Existing Analogs

### Model-Facing Manifest Boundary

- `model/colabs/common/manifest.py`
- Pattern: frozen dataclass for a row contract, conversion from raw manifest dicts, soft skipping of missing model-facing fields.
- Reuse: keep `CanonicalRow` stable and add dataset-versioning types separately.
- Do not reuse: `load_manifest()` soft-fails missing/unreadable local files to `[]`; Phase 1 user-configured GCS inputs need fail-fast errors.

### GCS Helpers

- `model/colabs/common/gcs_utils.py`
- Pattern: `parse_gcs_uri()`, `storage_client.bucket(bucket).blob(path)`, `download_as_text(retry=DEFAULT_RETRY)`.
- Reuse: parse and storage-client injection patterns.
- Extend: add strict JSON/JSONL parsing that raises contextual validation errors instead of skipping malformed input.

### Existing SFT Config Registry

- `model/scripts/sft/datasets.toml`
- `model/scripts/sft/pipeline.py`
- Pattern: TOML registry loaded with stdlib `tomllib`.
- Reuse: TOML for authored config and simple dataclass/dict loading.
- Do not reuse: existing registry is Gemini/round-oriented and already split-specific; Phase 1 config is dataset-version input oriented.

### Existing SFT Tests

- `model/scripts/sft/tests/test_pipeline_build.py`
- Pattern: `unittest`, local temp files, fake storage clients, `sys.path` setup for `model/scripts/sft` and `model/colabs`.
- Reuse: no-network tests with fake readers and direct module imports.

### Echo Source Registry

- `model/data_sources/echo/all_echo_mono_streams.csv`
- `model/data_sources/echo/s3_file_scanner.py`
- Pattern: Echo S3 key shape is `<area_code>/<YYYYMMDD>/<echo_name>_<YYYYMMDD>_<HH>.mp3`.
- Reuse: parse Echo area and echo name from explicit fields or URI, then validate against the built-in registry when needed.

### Broadcastify Feed Archive Shape

- `model/data_sources/broadcastify/archive_urls_sample_20260114_12hrs.csv`
- Pattern: archive URL includes feed ID as path segment and filename suffix.
- Reuse: `https://archives.broadcastify.com/<feedId>/<date>/<timestamp>-<id>-<feedId>.mp3` parser as fallback when explicit `feedId` is absent.

### Fire Notification Stream Shape

- `model/data_sources/fire_notifications/fetch_fn_archives_day.py`
- Pattern: stream name is `"/".join(fn_file.path.split("/")[:2])`.
- Reuse: `location`, `stream_path`, or first two original path components as stable source group.

## Planned Files

### Package

- `model/scripts/sft/dataset_versioning/__init__.py`
- `model/scripts/sft/dataset_versioning/config.py`
- `model/scripts/sft/dataset_versioning/gcs_io.py`
- `model/scripts/sft/dataset_versioning/types.py`
- `model/scripts/sft/dataset_versioning/source_keys.py`
- `model/scripts/sft/dataset_versioning/normalize.py`
- `model/scripts/sft/dataset_versioning/validate.py`
- `model/scripts/sft/validate_dataset_version.py`

### Tests

- `model/scripts/sft/tests/test_dataset_version_config.py`
- `model/scripts/sft/tests/test_dataset_version_gcs_io.py`
- `model/scripts/sft/tests/test_dataset_version_source_keys.py`
- `model/scripts/sft/tests/test_dataset_version_normalize.py`
- `model/scripts/sft/tests/test_dataset_version_validate.py`

## Data Flow

1. `validate_dataset_version.py` receives a TOML `--config-uri` argument. The config URI, dataset manifests, and source maps must be `gs://`; tests use fake readers rather than real GCS.
2. `config.py` parses TOML into `DatasetVersionConfig` and `InputDatasetConfig`.
3. `gcs_io.py` reads each configured manifest/source map from GCS through an injectable reader.
4. `normalize.py` converts raw rows to `LabeledSegment` or a soft exclusion.
5. `source_keys.py` resolves `source_group` with fixed strategy cascades.
6. `validate.py` prints a per-dataset CLI/log summary and fails first hard validation error with contextual details.

## Landmines

- Do not silently accept local manifest/source-map paths.
- Do not treat `echo_name` alone as safe, even when currently unique in a sample.
- Do not store `raw_row` in generated model-facing artifacts.
- Do not make source fallback order configurable in Phase 1.
- Do not modify existing benchmark/eval manifests or `model/scripts/sft/datasets.toml`.
