# Phase 2: Split Engine And Leakage Gates - Patterns

**Date:** 2026-05-27
**Status:** Complete

## Pattern Sources

- `model/scripts/sft/dataset_split/types.py`
- `model/scripts/sft/dataset_split/normalize.py`
- `model/scripts/sft/dataset_split/validate.py`
- `model/scripts/sft/dataset_split/config.py`
- `model/scripts/sft/tests/test_dataset_split_normalize.py`
- `model/scripts/sft/tests/test_dataset_split_validate.py`
- `model/pyproject.toml`
- `model/scripts/sft/requirements.txt`

## New Files And Closest Analogs

| New File | Role | Closest Existing Analog | Pattern To Reuse |
|----------|------|-------------------------|------------------|
| `model/scripts/sft/dataset_split/split.py` | Source-group assignment and split result types | `model/scripts/sft/dataset_split/validate.py` | Frozen dataclasses, small domain exception, fail-fast orchestration |
| `model/scripts/sft/dataset_split/leakage.py` | Hard split validators | `model/scripts/sft/dataset_split/source_keys.py` | Family-independent validation helpers that raise short contextual errors |
| `model/scripts/sft/dataset_split/balance.py` | Bucket metrics and report construction | `model/scripts/sft/dataset_split/normalize.py` | Pure functions over `LabeledSegment` rows with no external I/O |
| `model/scripts/sft/tests/test_dataset_split_split.py` | Split assignment tests | `model/scripts/sft/tests/test_dataset_split_validate.py` | Local builders, no network, `unittest` assertions |
| `model/scripts/sft/tests/test_dataset_split_leakage.py` | Leakage gate tests | `model/scripts/sft/tests/test_dataset_split_source_keys.py` | Focused valid/failure cases with exact expected messages |
| `model/scripts/sft/tests/test_dataset_split_balance.py` | Report and bucket tests | `model/scripts/sft/tests/test_dataset_split_normalize.py` | Construct small `LabeledSegment` fixtures directly |

## Existing Data Flow

1. `validate_dataset.py` reads config from GCS.
2. `validate.py` loads configured manifests and source maps.
3. `normalize.py` turns rows into `LabeledSegment` records.
4. Phase 2 should take `tuple[LabeledSegment, ...]`, assign `split`, validate leakage, and compute reports.

## Code Patterns To Preserve

- Keep modules in `model/scripts/sft/dataset_split/` and tests in `model/scripts/sft/tests/`.
- Use frozen dataclasses for data contracts.
- Use short domain exceptions subclassing `ValueError`.
- Keep tests local and fake all external I/O.
- Use `PYTHONPATH=model/scripts/sft:model/colabs python3 -m pytest ... -q` for SFT test commands.
- Do not modify `model/colabs/common/manifest.py` or existing historical eval manifests.

## Implementation Landmines

- Phase 1 still has `random_seed` in `DatasetVersionConfig`; Phase 2 context says to remove or ignore it if unused.
- `LabeledSegment.duration` can be `0.0`; bucket code must handle zero-duration rows deterministically.
- `timestamp` is optional and should be report-only. Missing or unparsable timestamps must not fail the split.
- `model_ready_audio_uri` is usually `None` in Phase 2. Leakage validators must ignore empty values for that field while still checking source and original audio.
- Same-split duplicate detection must not include `text`; two rows with the same audio span and different text are still duplicates.

## Test Strategy Pattern

Use small fixture builders:

```python
def _segment(
    source_group: str,
    *,
    dataset_name: str = "ds",
    dataset_family: str = "bcfy_calls",
    original_audio_uri: str | None = None,
    text: str = "alpha bravo",
    offset: float = 0.0,
    duration: float = 10.0,
) -> LabeledSegment:
    ...
```

Tests should assert exact behavior:

- all rows for one `source_group` have one split value
- `SplitAssignmentError` message includes the dataset name when fewer than two Source Groups exist
- leakage errors name the leaked field
- duplicate errors name `original_audio_uri`, `offset`, and `duration`
- report dict contains `weighted_score`, `component_deltas`, `duration_buckets`, `transcript_length_buckets`, and `time_distribution`
