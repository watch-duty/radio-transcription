# Phase 5: CLI, Reports, Docs, And Verification - Context

**Gathered:** 2026-05-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Expose the existing SFT dataset split machinery as a runbook-oriented CLI. The phase delivers `split_dataset.py` commands for dry-run and generate, report bundle improvements, minimal existing-doc updates, and targeted verification. It should not reopen source identity, split balancing, artifact layout, model writer formats, audio derivation policy, force/resume/cleanup, or SFT run execution.

</domain>

<decisions>
## Implementation Decisions

### Dry-Run Artifact Contract
- **D-01:** Dry-run writes a local plan bundle: reports plus previews of canonical manifests and NeMo/Whisper/Gemini model inputs.
- **D-02:** Dry-run does not upload audio and does not download, probe, copy, derive, transcode, or run ffmpeg.
- **D-03:** Dry-run preview rows use planned final `gs://` model-ready audio URIs so the previews match generate output paths.
- **D-04:** Dry-run reports must make the non-materialized state explicit, for example `audio_materialized=false`.
- **D-05:** Dry-run fails if the selected local `--output-dir` already exists.

### CLI Shape
- **D-06:** Add `model/scripts/sft/split_dataset.py` as the user-facing script.
- **D-07:** Expose only `dry-run` and `generate`; validation is an internal step in both commands, not a separate public command.
- **D-08:** Both commands are config-driven. `dry-run` requires `--config-uri` and `--output-dir`; `generate` requires `--config-uri`.
- **D-09:** Dataset manifests, source maps, ratios, output prefix, and model writer settings come from the TOML config, not CLI override flags.
- **D-10:** `generate` uses temporary scratch space by default. Do not add a public scratch-dir flag in this phase.

### Failure Behavior
- **D-11:** Do not write failure report files for `dry-run` or `generate`.
- **D-12:** Hard failures exit nonzero and print a short actionable error with the first failing dataset/context plus cause.
- **D-13:** Source-key failures are represented by the short CLI error. Successful reports can state `source_key_failures=0`.
- **D-14:** If `generate` fails after uploading some audio, leave the uploaded objects in place. Do not add cleanup, resume, force, or partial-state reports.
- **D-15:** This intentionally narrows the earlier "failure report location" wording: surfacing the short error is enough for this runbook script.

### Report Bundle
- **D-16:** Keep the summary report pair: `dataset_version_report.json` and `dataset_version_report.md`.
- **D-17:** Add row-level sidecars only where useful and non-duplicative.
- **D-18:** Successful runs produce `reports/excluded_rows.jsonl` for non-fatal exclusions, currently rows skipped for empty transcript/text.
- **D-19:** Do not add a transformations sidecar in this phase; transformation details remain in canonical manifests.
- **D-20:** Markdown report shows summaries and artifact paths only, not row-level tables or transcript/audio examples.

### Docs And Verification
- **D-21:** Treat `split_dataset.py` as a runbook script, not a production service.
- **D-22:** Use minimal automated tests: command dispatch, successful dry-run bundle shape, one hard failure surfacing a short error, and generate smoke dispatch/error handling.
- **D-23:** Do not add live GCS automated tests.
- **D-24:** Do not add new docs. Update existing docs or code comments only when the behavior is not evident from code or `--help`, and avoid duplicating implementation details that will drift.
- **D-25:** Final verification should run targeted SFT script tests and compile the new script.

### the agent's Discretion
- Choose the smallest internal factoring that makes `dry-run` and `generate` share validation/split/report assembly safely.
- Prefer simple, readable CLI output over a complex machine-readable CLI protocol.
- Existing artifact layout and model writer behavior should be reused directly unless the Phase 5 decisions above require a small adapter.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope
- `.planning/PROJECT.md` — Project goals, active Phase 5 focus, and prior milestone status.
- `.planning/REQUIREMENTS.md` — CLI-01 through CLI-05 requirements and traceability.
- `.planning/ROADMAP.md` — Phase 5 goal, success criteria, and planned work items.

### Prior Phase Decisions
- `.planning/phases/02-split-engine-and-leakage-gates/02-CONTEXT.md` — Split-before-derivation, balance priorities, leakage gates, and report-only balance behavior.
- `.planning/phases/03-artifact-structure-and-model-input-writers/03-CONTEXT.md` — Dataset-version GCS layout, canonical/model input artifacts, immutable publication, and dataset-version report boundary.
- `.planning/phases/04-audio-derivation-and-provenance/04-CONTEXT.md` — Audio preparation actions, model-ready URI requirements, provenance metadata, no force/resume/cleanup.

### Existing Code
- `model/scripts/sft/validate_dataset.py` — Current small validation CLI to replace or supersede with `split_dataset.py`.
- `model/scripts/sft/dataset_split/config.py` — TOML config parser and config-driven dataset inputs.
- `model/scripts/sft/dataset_split/validate.py` — Dataset validation, source map loading, normalization summaries, and non-fatal exclusions.
- `model/scripts/sft/dataset_split/split.py` — Source-group split assignment and balance report generation.
- `model/scripts/sft/dataset_split/publisher.py` — Existing generate path for GCS artifact publication.
- `model/scripts/sft/dataset_split/audio.py` — Current audio preparation behavior; dry-run must avoid invoking materialization.
- `model/scripts/sft/dataset_split/reports.py` — Summary report builder and Markdown renderer.
- `model/scripts/sft/dataset_split/canonical.py` — Canonical manifest rows where transformation metadata already lives.
- `model/scripts/sft/dataset_split/leakage.py` — Leakage and model-ready validation gates.
- `model/scripts/sft/README.md` — Existing SFT docs to update only if needed.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `parse_dataset_version_config_toml`: already loads the TOML config that should be the only public input pointer.
- `validate_dataset` and `normalize_manifest_rows`: already produce valid segments and excluded empty-text rows.
- `assign_train_eval_split`: already computes whole-source-group train/eval assignment and balance report.
- `publish_dataset_version_artifacts`: already writes production GCS artifacts for generate.
- `build_dataset_version_report` and `render_dataset_version_markdown`: already provide the summary report pair to extend with sidecar inventory and dry-run state.

### Established Patterns
- SFT script tests live under `model/scripts/sft/tests` and use fake clients rather than live cloud state.
- CLI workflows should return integer status codes and print clean user-facing messages for expected failures.
- Generated artifacts/audio live in GCS or local dry-run output, not Git.
- Existing code prefers fail-fast validation for hard data-shape and source-key failures.

### Integration Points
- `split_dataset.py dry-run` connects config loading, validation, split assignment, plan-only model-ready URI enrichment, report writing, canonical/model input preview writing, and local output-dir checks.
- `split_dataset.py generate` connects config loading, validation, split assignment, leakage validation, publisher invocation, and concise success/failure output.
- Report bundle changes connect `validate_dataset` excluded rows to `reports/excluded_rows.jsonl` and add sidecar paths/counts to existing reports.

</code_context>

<specifics>
## Specific Ideas

- The CLI name should be `split_dataset.py` because the main task is splitting existing datasets into train and eval, not generic dataset versioning.
- The dry-run output is a plan bundle, not a local publishable dataset-version mirror.
- The script is a runbook tool; simple error surfacing is acceptable over durable failure artifacts.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 5-CLI, Reports, Docs, And Verification*
*Context gathered: 2026-05-28*
