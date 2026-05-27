# Phase 2: Split Engine And Leakage Gates - Context

**Gathered:** 2026-05-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 turns Phase 1 `LabeledSegment` rows into train and SFT Eval Split assignments. It assigns whole Source Groups to exactly one split, prioritizes balance quality, validates no-leak postconditions, and reports split quality. It does not write GCS artifacts, generate model-specific manifests, derive audio clips, or run tuning jobs.

</domain>

<decisions>
## Implementation Decisions

### Split Objective
- **D-01:** Optimize for per-dataset train/eval coverage first. Each configured dataset should have train and SFT Eval Split representation when it has enough Source Groups.
- **D-02:** Fail fast when a configured dataset has fewer than two Source Groups, because it cannot support both splits without violating Source Group separation.
- **D-03:** Balance quality is higher priority than deterministic split reproduction. Current requirement `SPLT-02` should be revised: reproducibility should come from saving the chosen assignment, score report, config, and algorithm metadata, not from requiring the algorithm to produce the same split for a seed.
- **D-04:** Do not add an optimizer interface. Implement one direct balance-first splitter.
- **D-05:** Do not preserve `random_seed` as ceremony. If the selected splitter does not use randomness, remove or ignore `random_seed` in the split path and update config/tests accordingly.

### Leakage Gates
- **D-06:** Split assignment happens before downstream clip derivation or segmentation. Hard leakage validators run after assignment as defensive postcondition checks.
- **D-07:** Hard fail exact cross-split overlap for `source_group`, `original_audio_uri`, and non-empty `model_ready_audio_uri`.
- **D-08:** Skip fuzzy/audio-content duplicate detection and URI alias detection in Phase 2.
- **D-09:** Hard fail exact duplicate labeled audio spans within a split using `(original_audio_uri, offset, duration)`. Do not include transcript text in the duplicate key.
- **D-10:** Compare URIs by exact normalized string match only: strip whitespace and compare the resulting strings.

### Balance Scoring And Reports
- **D-11:** Score factors that can affect transcription performance: dataset name/family, row count, audio duration, source count, duration buckets, and transcript-length buckets.
- **D-12:** Time/month/hour balance is report-only when timestamps are available. It must not block or drive split assignment.
- **D-13:** Use fixed built-in buckets rather than TOML-configurable buckets or per-dataset quantile buckets.
- **D-14:** Transcript-length buckets use normalized word count.
- **D-15:** Balance quality is not a hard gate. Hard failures are leakage, exact duplicate labeled audio spans, zero valid dataset, and impossible split conditions. Imperfect balance is reported.
- **D-16:** Reports should include one weighted balance score plus component deltas so humans can audit why the selected split won.

### the agent's Discretion
The planner may choose the concrete balance algorithm and built-in bucket boundaries, but it must evaluate algorithm options before implementation and choose the best practical single implementation for balance quality. No broad plugin/interface abstraction was granted.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope And Prior Decisions
- `.planning/PROJECT.md` — Core value, glossary, source-group decisions, GCS artifact boundary, and validated dataset observations.
- `.planning/REQUIREMENTS.md` — Phase 2 requirements. Note that `SPLT-02` and `random_seed` assumptions are superseded by this context and should be revised during planning/implementation.
- `.planning/ROADMAP.md` — Phase boundary, success criteria, and Phase 2 plan breakdown.
- `.planning/STATE.md` — Current project state and recent decision summary.
- `.planning/phases/01-manifest-and-source-identity/01-CONTEXT.md` — Locked Phase 1 decisions for config, source identity, row normalization, empty-text exclusions, and fail-fast validation.

### Codebase Maps
- `.planning/codebase/ARCHITECTURE.md` — Offline SFT tooling integration point and model/evaluation layer boundaries.
- `.planning/codebase/CONVENTIONS.md` — Python naming, dataclass, model/SFT module, error handling, and style conventions.
- `.planning/codebase/TESTING.md` — Model/SFT test location and focused regression-test patterns.

### Existing Phase 1 Code
- `model/scripts/sft/dataset_split/types.py` — `LabeledSegment`, validation exceptions, and fields available for split/leakage/balance logic.
- `model/scripts/sft/dataset_split/normalize.py` — Normalized row construction, `original_audio_uri` fallback, text normalization, offsets, durations, and timestamps.
- `model/scripts/sft/dataset_split/validate.py` — Current dataset validation boundary and zero-valid-example behavior.
- `model/scripts/sft/dataset_split/config.py` — Current config parser, including `random_seed`; Phase 2 may need to revise this if the splitter does not use seed.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `LabeledSegment` already contains `dataset_name`, `dataset_family`, `source_group`, `audio_uri`, `original_audio_uri`, `text`, `offset`, `duration`, `timestamp`, `split`, and `model_ready_audio_uri`. Phase 2 should operate on this type instead of inventing a parallel row shape.
- `normalize_text()` already strips line breaks and whitespace. Transcript word-count buckets should use normalized text from `LabeledSegment.text`.
- `DatasetValidationError` and fail-fast validation style already exist. Phase 2 hard gates should use short contextual errors, not large cleanup reports.

### Established Patterns
- Model/SFT tooling uses small Python modules under `model/scripts/sft` and focused pytest tests under `model/scripts/sft/tests`.
- Existing Phase 1 behavior treats empty text as a soft exclusion, but a dataset with zero valid examples is a hard failure.
- Existing config is TOML for user-authored input and JSON/JSONL for generated/resolved state and reports.

### Integration Points
- Add split, leakage, and balance logic under `model/scripts/sft/dataset_split/`.
- Extend or add tests under `model/scripts/sft/tests/`.
- Later phases consume Phase 2 split assignments for artifact writing, model writers, and audio derivation; Phase 2 should not perform those later-phase transformations.

</code_context>

<specifics>
## Specific Ideas

- Split assignment should group by `source_group` before any clip derivation.
- Defensive leakage gates are exact set intersections over split membership.
- Same-split duplicate failures use audio span identity, not transcript text.
- Fixed duration buckets should cover short, medium, Whisper-sensitive, and long examples. Exact boundaries are left to the planner.
- Balance research consulted scikit-learn group and stratified split documentation: grouped splitting prevents group overlap, while stratification preserves distributions where possible. Phase 2 adapts that idea to ASR performance-correlated buckets instead of class labels.

</specifics>

<deferred>
## Deferred Ideas

- GCS artifact layout, model-specific writers, and immutable dataset output remain Phase 3.
- Audio reuse/derivation, clipping, transformation metadata, and provenance completion remain Phase 4.
- Actual NeMo, Whisper, and Gemini tuning job submission remains v2.

</deferred>

---

*Phase: 2-Split Engine And Leakage Gates*
*Context gathered: 2026-05-27*
