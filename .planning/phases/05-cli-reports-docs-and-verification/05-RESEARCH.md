# Phase 5: CLI, Reports, Docs, And Verification - Research

## RESEARCH COMPLETE

### Scope

Phase 5 should expose the existing dataset split and publication machinery as a small runbook CLI. It should not redesign source identity, split optimization, model writer formats, or Phase 4 audio materialization.

### Existing Assets

- `model/scripts/sft/validate_dataset.py` is the current small CLI. It only validates configured GCS manifests and prints summary lines.
- `dataset_split.config.parse_dataset_version_config_toml()` already owns the TOML contract: dataset version id, train/eval ratios, output GCS prefix, datasets, manifest URIs, source strategies, and optional source maps.
- `dataset_split.validate.validate_dataset()` currently returns only `DatasetValidationSummary` values. Phase 5 needs the underlying normalized `segments` and non-fatal `excluded` rows, so add a loader result and keep `validate_dataset()` as a compatibility wrapper only if the file remains.
- `dataset_split.split.assign_train_eval_split()` already computes whole-source-group train/eval assignment and returns balance metadata/report.
- `dataset_split.publisher.publish_dataset_version_artifacts()` already handles the production `generate` path: immutable root check, audio preparation, canonical/per-dataset/model inputs, metadata, report, and uploads.
- `dataset_split.reports.build_dataset_version_report()` and `render_dataset_version_markdown()` already produce the summary report pair. They should be extended rather than replaced.
- `dataset_split.canonical.canonical_manifests()` validates model-ready audio and transformation metadata. Dry-run must avoid accidentally invoking full audio materialization while still producing useful previews.
- Existing SFT tests under `model/scripts/sft/tests` use fake readers/storage clients and direct `main(argv)` calls. Keep Phase 5 tests in that style.

### Implementation Findings

1. `split_dataset.py` should replace the public validation script surface.
   - Commands: `dry-run` and `generate`.
   - `dry-run`: `--config-uri`, `--output-dir`.
   - `generate`: `--config-uri`.
   - No public `validate` command.

2. Both commands should share a small orchestration layer.
   - Load config from `gs://`.
   - Read and normalize all manifests.
   - Preserve both valid `LabeledSegment` rows and non-fatal `ExcludedRow` rows.
   - Assign split.
   - Validate leakage after split.
   - Build reports and model inputs.

3. Dry-run needs a plan-only audio enrichment path.
   - Do not call `prepare_audio_for_publication()`, `plan_audio_actions()`, `stage_source_audio()`, `probe_audio()`, or ffmpeg/ffprobe.
   - Enrich rows with planned final `gs://` model-ready URIs so NeMo/Whisper/Gemini previews are useful.
   - Mark report metadata with `audio_materialized=false`.
   - Include enough placeholder transformation metadata for previews/reports without pretending source audio was probed. If full production validators require probed fields, dry-run should use preview-specific builders rather than weakening production validators.

4. Generate should reuse the publisher.
   - `publish_dataset_version_artifacts()` already checks the dataset-version prefix once and then uploads audio before text artifacts.
   - If publication fails after audio upload, Phase 5 should not add cleanup/resume/force.
   - CLI should print the dataset-version root and key report/model-input paths on success.

5. Failure UX should be intentionally small.
   - No failure report files.
   - No partial dry-run output directory on failure.
   - Catch expected domain errors and print the first short actionable message.
   - Let unexpected exceptions return a nonzero CLI status without adding long custom reports.

6. Report sidecar addition is narrow.
   - Add `reports/excluded_rows.jsonl` for successful runs.
   - Include only non-fatal exclusions such as `empty_text`.
   - Keep transformation row details in canonical manifests.
   - Source-key failures are fail-fast errors; successful reports can state `source_key_failures=0`.
   - Markdown should show summaries and artifact paths, not row tables.

7. Minimal docs are enough.
   - Do not add a new doc file.
   - Update `model/scripts/sft/README.md` only if `--help` and code cannot reveal a user-facing behavior or term.
   - Avoid duplicating the full artifact tree in prose; the CLI and report inventory should own exact paths.

### Risks And Mitigations

| Risk | Mitigation |
|------|------------|
| Dry-run accidentally downloads/probes/derives audio | Keep dry-run on a separate plan-only helper and test that the audio preparer/materializer is not called. |
| Dry-run preview URIs drift from generate paths | Use `DatasetArtifactLayout` and `audio_object_uri()` where possible; explicitly mark preview reports `audio_materialized=false` so dry-run remains a plan, not a published guarantee. |
| Existing `validate_dataset.py` tests conflict with the new public CLI | Move CLI tests to `split_dataset.py`; keep validation module tests for library behavior only. |
| Report grows into a duplicated canonical manifest | Put row details only in `excluded_rows.jsonl`; keep transformations in canonical manifests. |
| GCS tests become brittle | Use fake readers/storage clients; no live GCS automated tests. |
| CLI-03 wording mentions report location, but context decided no failure reports | Plan must honor Phase 05 context: short nonzero error is sufficient for this runbook script. |

## Validation Architecture

### Test Layers

- Unit tests for shared loading/orchestration helpers:
  - config read failure and manifest validation failure surface short errors
  - split assignment is invoked after valid rows are loaded
  - excluded rows are preserved for report sidecars
- CLI smoke tests for `split_dataset.py`:
  - `dry-run --config-uri gs://... --output-dir ...` writes the expected local bundle shape for fixture data
  - existing output directory fails before writing
  - `generate --config-uri gs://...` dispatches to generation and converts expected errors to nonzero short output
- Report tests:
  - report inventory includes `reports/excluded_rows.jsonl`
  - Markdown includes counts and paths, not row-level tables
  - successful report states `source_key_failures=0`

### Verification Commands

Use targeted SFT script verification:

```bash
python3 -m py_compile model/scripts/sft/split_dataset.py
python3 -m pytest model/scripts/sft/tests -q
```

If the full SFT script test suite is too slow for an execution checkpoint, run the focused test files touched by the phase first, then run the full targeted suite before completion.

### Manual Checks

- Review `python model/scripts/sft/split_dataset.py --help`.
- Review `python model/scripts/sft/split_dataset.py dry-run --help`.
- Review `python model/scripts/sft/split_dataset.py generate --help`.

