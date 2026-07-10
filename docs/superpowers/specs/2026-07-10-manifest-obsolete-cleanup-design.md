# Manifest Obsolete Cleanup Design

## Goal

Remove migration-era manifest test cases, fixtures, and wording that describe
obsolete or deprecated behavior. Keep behaviors that remain part of the current
public contract or are required by tracked manifests and maintained notebooks.

## Scope

The cleanup is limited to `model/tests/common/tests/test_manifest.py` unless
removing obsolete coverage exposes unreachable production code. No current
production behavior is expected to change.

Remove:

- Named removed-field cases (`audio_processing`, `sequence`,
  `original_audio_uri`, `source_group`, `dataset_name`, and `lang`) from the
  generic unknown-field validation test.
- The unused `audio_processing` fixture in aggregated validation coverage.
- The redundant test that only proves the historical merge `return []` path no
  longer exists.
- Historical wording such as `legacy`, `older`, `re-raise`, `silently`, and
  `compatibility coercion` where the test protects a current contract.

Retain:

- Generic unknown row-field and metadata-extension coverage.
- `rows_from_manifest()` identity and offset defaults. Current tracked
  manifests omit these fields and the PR documents this compatibility API.
- Identity-free URI/offset prediction matching, which current artifacts and
  notebooks use.
- Fail-loud parsing and faithful row-value loading.
- Empty-hypothesis handling for null prediction text.
- Single-consumption, URI-first matching, stale-prediction clearing, and strict
  packaged-flow validation.

## Test Structure

Tests should describe current behavior directly rather than compare it with
removed implementations. Names, fixture values, class descriptions, and
comments should use current domain terminology. Generic extension tests should
use one prediction field and one neutral future field instead of enumerating
deprecated field names.

## Verification

Run:

```bash
safe-run -- env PYTHONPATH=model/src python3 -m pytest \
  model/tests/common/tests/test_manifest.py -q
safe-run -- uv run ruff check \
  model/src/common/manifest.py model/tests/common/tests/test_manifest.py
safe-run -- uv run ruff format --check \
  model/src/common/manifest.py model/tests/common/tests/test_manifest.py
git diff --check
```

The cleanup is complete when obsolete names and transition-only assertions are
absent, current-contract coverage remains, and all checks pass.
