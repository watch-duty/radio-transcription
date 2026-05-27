# Phase 2: Split Engine And Leakage Gates - Research

**Date:** 2026-05-27
**Status:** Complete

## Research Goal

Answer: what is the best practical way to plan Phase 2 so train/eval assignment is leak-safe and balanced across ASR performance-correlated factors?

Phase 2 starts from Phase 1 `LabeledSegment` rows and must assign whole Source Groups to train or SFT Eval Split, then validate leakage gates and report balance quality.

## Inputs Read

- `.planning/phases/02-split-engine-and-leakage-gates/02-CONTEXT.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `model/scripts/sft/dataset_split/types.py`
- `model/scripts/sft/dataset_split/normalize.py`
- `model/scripts/sft/dataset_split/validate.py`
- `model/scripts/sft/dataset_split/config.py`
- `model/pyproject.toml`
- `model/scripts/sft/requirements.txt`

## Key Existing Constraints

- `LabeledSegment` already has the fields needed for Phase 2: `dataset_name`, `dataset_family`, `source_group`, `original_audio_uri`, `audio_uri`, `text`, `offset`, `duration`, `timestamp`, `split`, and `model_ready_audio_uri`.
- Phase 1 sets `original_audio_uri` to the row value when present, otherwise to the row audio URI.
- Phase 1 config still requires `random_seed`, but Phase 2 context supersedes that: if the chosen splitter does not use randomness, seed should be removed or ignored.
- The root project targets Python 3.13, while local `python3` is 3.12. OR-Tools 9.15 publishes both cp312 and cp313 manylinux wheels; `pip download` confirmed cp313 wheel availability for `ortools==9.15.6755`.

## Algorithm Options Evaluated

### Single seeded shuffle / greedy fill

Reject. It is simple and reproducible, but it cannot reliably balance row count, duration, source count, duration buckets, transcript-length buckets, dataset name, and family at the same time.

### scikit-learn group splitters

Useful as conceptual background, but not a good implementation fit. `GroupShuffleSplit` and `StratifiedGroupKFold` preserve non-overlapping groups and label distributions where possible, but the phase needs a custom weighted multi-factor objective and a single train/eval assignment, not a standard cross-validation iterator.

Reference:
- `https://scikit-learn.org/stable/modules/cross_validation.html`
- `https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.StratifiedGroupKFold.html`

### Exhaustive search

Best for tiny datasets, but not viable for the expected source counts. Existing observations include 57 Broadcastify Calls groups and 115 Broadcastify Feeds feed IDs, and Echo can involve hundreds of devices. Exhaustive assignment is exponential in Source Groups.

### Multi-start local search / simulated annealing

Practical and dependency-light, but it provides no optimality signal and can require hand-tuning. It is a reasonable fallback if solver dependencies are disallowed, but the user prioritized balance over determinism and did not request avoiding dependencies.

### CP-SAT / integer optimization

Selected. Binary decision variables per Source Group directly model train/eval assignment. Hard constraints can enforce at least one train and one eval Source Group per configured dataset. Absolute-delta variables can model deviations from target ratios for rows, duration, source count, and bucket counts. A weighted linear objective produces a single auditable score.

OR-Tools CP-SAT docs confirm the Python API supports integer variables, linear constraints, linear objectives, and feasible/optimal status reporting through `cp_model.CpModel()` and `cp_model.CpSolver()`.

Reference:
- `https://developers.google.com/optimization/cp/integer_opt_cp`

## Selected Technical Approach

Use one direct OR-Tools CP-SAT implementation. Do not introduce a generic optimizer interface.

Core model:

- One Boolean variable per Source Group: `is_eval[group]`.
- Train membership is the complement.
- For each configured dataset with `n` Source Groups:
  - hard constraint: `sum(is_eval[group]) >= 1`
  - hard constraint: `sum(is_eval[group]) <= n - 1`
  - if `n < 2`, fail fast before solving.
- Objective minimizes weighted absolute deviation from the configured eval ratio for:
  - dataset name row counts
  - dataset name durations
  - dataset name source counts
  - dataset family row counts
  - dataset family durations
  - global row count
  - global duration
  - global source count
  - fixed audio-duration bucket counts
  - fixed transcript-word-count bucket counts
- Timestamp/month/hour distributions are report-only and do not affect the objective.

Implementation notes:

- CP-SAT uses integer coefficients, so durations should be quantized to milliseconds or centiseconds before entering the model.
- The solver may return `OPTIMAL` or `FEASIBLE`; both are acceptable if the report records solver status, objective value, elapsed time, and weights.
- The split assignment artifact is the reproducibility contract. The planner should update the code/tests so `random_seed` is not required if unused.

## Leakage Gate Approach

Run exact post-assignment validators:

- Cross-split overlap in `source_group` is a hard failure.
- Cross-split overlap in `original_audio_uri` is a hard failure.
- Cross-split overlap in non-empty `model_ready_audio_uri` is a hard failure.
- Same-split exact duplicate labeled audio spans are hard failures using `(original_audio_uri, offset, duration)`.
- URI matching is exact normalized string matching: `str(value).strip()`.
- Do not implement fuzzy matching, signed URL canonicalization, or audio-content duplicate detection in Phase 2.

## Report Approach

Reports should contain:

- requested train/eval ratio
- actual row ratio
- actual duration ratio
- actual source ratio
- weighted balance score
- component deltas for every scored factor
- duration bucket deltas
- transcript word-count bucket deltas
- timestamp/month/hour distribution when timestamps are present, marked report-only
- leakage validation summary
- duplicate audio span validation summary
- algorithm metadata: implementation name, OR-Tools status, objective value, weights, and solver time limit

## Validation Architecture

Automated tests should cover:

- CP-SAT assignment keeps every Source Group wholly in one split.
- Datasets with fewer than two Source Groups fail fast.
- Split assignment gives each eligible dataset both train and eval examples.
- The chosen split optimizes a weighted score, not a single seeded shuffle.
- Config no longer requires `random_seed` if the splitter does not use it.
- Cross-split source/original/model-ready URI overlap failures.
- Same-split duplicate audio span failures using audio fields only.
- Exact URI comparison behavior.
- Duration bucket and transcript word-count bucket scoring.
- Time/month/hour distribution report-only behavior.
- Full Phase 2 pytest suite passes without GCS, Broadcastify, Echo S3, Fire Notifications, or audio downloads.

## Risks

- OR-Tools adds a heavy dependency. Mitigation: pin `ortools>=9.15,<10`, add it to the model/SFT install path, and verify import in tests.
- CP-SAT objective weights can hide a poor component. Mitigation: report component deltas alongside the single weighted score.
- Solver time limits can return a feasible but not optimal solution. Mitigation: record solver status and objective value in the report; balance is not a hard gate.
- Existing roadmap/requirements wording still says deterministic. Mitigation: Phase 2 plans must cite the context decision and update or supersede deterministic seed assumptions during implementation.

## Output Files To Plan

- `model/scripts/sft/dataset_split/split.py`
- `model/scripts/sft/dataset_split/leakage.py`
- `model/scripts/sft/dataset_split/balance.py`
- `model/scripts/sft/tests/test_dataset_split_split.py`
- `model/scripts/sft/tests/test_dataset_split_leakage.py`
- `model/scripts/sft/tests/test_dataset_split_balance.py`
- `model/pyproject.toml`
- `model/scripts/sft/requirements.txt`
- `model/scripts/sft/dataset_split/config.py`
- `model/scripts/sft/tests/test_dataset_split_config.py`
