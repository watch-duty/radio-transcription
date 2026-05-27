# Phase 1: Manifest And Source Identity - Research

## Research Complete

### Planning Question

What does Phase 1 need so implementation can safely load configured dataset-version inputs, normalize source-aware rows, and resolve leak-safe Source Groups before the split engine exists?

### Findings

1. The existing model tooling already has a model-facing manifest boundary in `model/colabs/common/manifest.py`. `CanonicalRow` is intentionally small and should stay stable for eval/SFT consumers.
2. Phase 1 needs a new internal row shape because source identity, original audio identity, row-index diagnostics, nullable future provenance fields, and exclusion status do not fit `CanonicalRow`.
3. The existing GCS helper `model/colabs/common/gcs_utils.py` already provides `parse_gcs_uri()` and `download_jsonl_manifest()`, but the Phase 1 loader needs stricter error behavior than the current helper: bad or unreadable user-configured manifests should fail fast, not return `[]` or skip malformed lines silently.
4. Context7 current Google Cloud Storage Python docs confirm the storage client supports `blob.download_as_text()` for in-memory text reads and `blob.open("r")` for file-like reads. The docs also identify `google.cloud.exceptions.NotFound` as a missing-object failure for blob download operations.
5. Echo source identity must use `area_code` plus `echo_name`. The repo snapshot `model/data_sources/echo/all_echo_mono_streams.csv` contains duplicate `echo_name` values; `Tehama_Sheriff_Disp` appears under both `ca_chico` and `ca_red_bluff`.
6. Broadcastify Feeds archive URLs include feed ID in both path and filename, e.g. `https://archives.broadcastify.com/119/20260114/202601141312-685630-119.mp3`. This supports a deterministic fallback parser for `bcfy_feeds`, but explicit fields should remain preferred.
7. Fire Notifications sampling code emits stream/location information as the first two path components, e.g. `TEXAS/<stream>`, and the collection day UUID is only a sampling artifact.

## Implementation Architecture

### Module Placement

Create a small package under `model/scripts/sft/dataset_versioning/` rather than extending `pipeline.py`. This keeps dataset-version input validation separate from the existing Gemini-oriented SFT build/tune/eval CLI.

Suggested modules:

- `config.py`: TOML config dataclasses and parser.
- `gcs_io.py`: strict `gs://` manifest/source-map readers with injectable fake reader support.
- `types.py`: `LabeledSegment`, validation/exclusion models, and summary dataclasses.
- `source_keys.py`: fixed source-strategy extractor cascades.
- `normalize.py`: manifest row to `LabeledSegment` validation and soft empty-text exclusions.
- `validate.py`: orchestration function and small CLI entrypoint used for Phase 1 validation.

### Config Contract

User-authored config should be TOML:

```toml
dataset_version_id = "sft_v1"
random_seed = 42
train_ratio = 0.8
eval_ratio = 0.2
output_gcs_prefix = "gs://wd-transcription-data/sft/sft_v1/"

[[datasets]]
name = "echo_30h"
family = "echo"
manifest_uri = "gs://bucket/path/echo.jsonl"
source_strategy = "echo"
source_map_uri = "gs://bucket/path/echo_source_map.jsonl"
```

Phase 1 validates the config and reads manifests; later phases consume the same config for splitting and artifact writing.

### Source Strategy Rules

- `bcfy_calls`: source group is `bcfy_calls:<groupId>`.
- `bcfy_feeds`: source group is `bcfy_feeds:<feedId>`.
- `echo`: source group is `echo:<area_code>/<echo_name>`.
- `fire_notifications`: source group is `fire_notifications:<stream_path>`.

Every strategy has a fixed cascade. Do not allow per-dataset fallback order yet.

## Validation Architecture

### Test Scope

Phase 1 should use focused unit tests with fake GCS readers. No test should require a real bucket, credentials, external network, or sampled audio.

### Commands

- Config/loading tests: `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_config.py model/scripts/sft/tests/test_dataset_version_gcs_io.py -q`
- Source/normalization tests: `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_source_keys.py model/scripts/sft/tests/test_dataset_version_normalize.py -q`
- Phase 1 validation tests: `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_validate.py -q`
- Full Phase 1 suite: `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest model/scripts/sft/tests/test_dataset_version_config.py model/scripts/sft/tests/test_dataset_version_gcs_io.py model/scripts/sft/tests/test_dataset_version_source_keys.py model/scripts/sft/tests/test_dataset_version_normalize.py model/scripts/sft/tests/test_dataset_version_validate.py -q`

### Hard Failure Cases

- Non-`gs://` manifest or source-map URI.
- Missing required config field.
- Unknown `family` or `source_strategy`.
- Missing/unreadable GCS object.
- Malformed JSON/JSONL manifest object.
- Unresolved or ambiguous source identity.
- Dataset with zero valid examples after empty-text exclusions.

### Soft Exclusion Cases

- Missing `text`.
- `text: null`.
- Text that normalizes to empty or whitespace.

Soft exclusions are counted in the CLI/log summary.

## Threat And Safety Notes

- Manifest rows are untrusted input. Treat source identity as data validation, not best-effort parsing.
- Do not place `raw_row` in generated model artifacts. It may be retained internally and summarized in failure diagnostics only.
- Do not contact third-party Broadcastify, Echo S3, or Fire Notifications APIs in Phase 1 tests.
- Do not log credentials or full raw rows. Failure messages should include dataset name, manifest URI, row index, audio URI, strategy, reason, and relevant source fields.

## Research Sources

- Context7 `/googleapis/python-storage` docs for `download_as_text()`, `blob.open("r")`, and missing-object exceptions.
- `model/colabs/common/manifest.py`
- `model/colabs/common/gcs_utils.py`
- `model/scripts/sft/pipeline.py`
- `model/data_sources/echo/README.md`
- `model/data_sources/echo/all_echo_mono_streams.csv`
- `model/data_sources/broadcastify/archive_urls_sample_20260114_12hrs.csv`
- `model/data_sources/fire_notifications/fetch_fn_archives_day.py`
