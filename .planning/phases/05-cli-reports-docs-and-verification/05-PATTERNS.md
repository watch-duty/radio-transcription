# Phase 05 — Pattern Map

## Pattern Mapping Complete

### CLI Script Pattern

- **Closest analog:** `model/scripts/sft/validate_dataset.py`
- **Use for:** `argparse` setup, `main(argv) -> int`, `_make_text_reader()` importing Google Cloud lazily, short `print(str(exc))` failure handling.
- **Do not keep:** public `--config-uri` validation-only behavior as the main user path.

### GCS Reader Pattern

- **Closest analog:** `model/scripts/sft/dataset_split/gcs_io.py`
- **Use for:** `GoogleCloudTextReader`, `read_json_or_jsonl`, and `GcsInputError` wrapping.
- **Test pattern:** fake reader object with `read_text(uri)` as used in `test_dataset_split_validate.py`.

### Split And Report Pattern

- **Closest analogs:** `dataset_split/split.py`, `dataset_split/reports.py`, `dataset_split/publisher.py`
- **Use for:** `SplitResult.balance_report`, immutable `DatasetArtifactLayout`, summary report rendering, model writer summaries, and writer warnings.
- **Extension point:** report artifact inventory should include `reports/excluded_rows.jsonl`; Markdown should show sidecar counts/paths only.

### Production Publication Pattern

- **Closest analog:** `publish_dataset_version_artifacts()`
- **Use for:** generate command. It already handles create-only GCS publication and Phase 4 audio preparation.
- **Do not add:** force, resume, cleanup, partial publish report, or live GCS tests.

### Test Pattern

- **Closest analogs:** `test_dataset_split_validate.py`, `test_dataset_publisher.py`, `test_model_writers.py`
- **Use for:** direct `main(argv)` calls, `redirect_stdout`, `unittest.mock.patch`, fake readers/storage clients, and focused assertions on JSONL/report content.
- **New fixture file:** `model/scripts/sft/tests/test_split_dataset_cli.py`.

