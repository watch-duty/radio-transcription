# Phase 2: Split Engine And Leakage Gates - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-27
**Phase:** 2-Split Engine And Leakage Gates
**Areas discussed:** Split Objective, Leakage Gates, Balance Reports

---

## Split Objective

| Option | Description | Selected |
|--------|-------------|----------|
| Per-dataset 80:20 coverage | Each configured dataset should get train and eval coverage when it has enough Source Groups. | yes |
| Global 80:20 only | Optimize the whole combined corpus ratio first. | |
| Duration-first 80:20 | Optimize train/eval audio duration closest to 80:20. | |
| Row-count-first 80:20 | Optimize number of SFT examples closest to 80:20. | |

**User's choice:** Per-dataset 80:20 coverage.
**Notes:** User wants train/eval balance per configured dataset when possible.

| Option | Description | Selected |
|--------|-------------|----------|
| Fail if fewer than 2 Source Groups | Stop when a dataset cannot support both splits without source leakage. | yes |
| Allow train-only with warning | Continue but omit that dataset from eval. | |
| Exclude that dataset with warning | Remove the dataset from generation. | |
| Allow eval-only with warning | Use only for eval-style datasets. | |

**User's choice:** Fail if fewer than two Source Groups.
**Notes:** Simpler and leak-safe.

| Option | Description | Selected |
|--------|-------------|----------|
| Seeded multi-candidate search | Deterministically generate candidates and choose the best. | |
| Single seeded shuffle | Shuffle once and greedily fill eval. | |
| Exhaustive small-dataset search | Exact for small datasets, fallback for large datasets. | |
| Balance-first algorithm selection | Evaluate possible algorithms and choose the best one for balance quality. | yes |

**User's choice:** Balance is much higher priority than determinism.
**Notes:** Determinism is no longer a hard requirement. Reproducibility should come from saved assignment/report artifacts.

| Option | Description | Selected |
|--------|-------------|----------|
| Pluggable optimizer interface | Multiple optimizer backends behind an interface. | |
| Single direct implementation | One concrete balance-first splitter. | yes |

**User's choice:** No optimizer interface.
**Notes:** User said there will only be one implementation.

| Option | Description | Selected |
|--------|-------------|----------|
| Keep seed, but balance wins | Use seed only if helpful. | |
| Ignore/remove seed | Remove or ignore `random_seed` if not used. | yes |
| Require deterministic output for same seed | Preserve same-output behavior for same config and seed. | |

**User's choice:** Ignore or remove seed if unused.
**Notes:** Avoid retaining unused configuration fields.

---

## Leakage Gates

| Option | Description | Selected |
|--------|-------------|----------|
| Hard fail all cross-split overlaps | Check `source_group`, `original_audio_uri`, and non-empty `model_ready_audio_uri`. | yes |
| Hard fail source group only | Warn on URI overlap. | |
| Hard fail source and original audio | Defer model-ready URI validation. | |

**User's choice:** Hard fail exact cross-split overlaps.
**Notes:** User questioned whether this can happen if splitting is done before segmentation. Decision: keep these as cheap defensive postcondition checks.

| Option | Description | Selected |
|--------|-------------|----------|
| Exact overlap checks only | Set intersections over normalized identifiers. | yes |
| Complex/fuzzy duplicate detection | Alias resolution or audio-content similarity. | |

**User's choice:** Exact checks only.
**Notes:** Skip fuzzy/audio-content duplicate detection in Phase 2.

| Option | Description | Selected |
|--------|-------------|----------|
| Report same-split duplicates only | Count duplicates but continue. | |
| Hard fail exact duplicate rows | Stop on duplicates within a split. | yes |
| Ignore same-split duplicates | No duplicate reporting. | |

**User's choice:** Hard fail exact duplicate rows within the same split.
**Notes:** Duplicate definition should be audio-related only.

| Option | Description | Selected |
|--------|-------------|----------|
| Audio identity + offset/duration + text | Includes transcript text. | |
| Audio URI + text only | Simpler but ignores offsets. | |
| Full normalized row fields | Strict but misses metadata-only duplicate changes. | |
| Audio span identity | `original_audio_uri`, `offset`, and `duration`; no text. | yes |

**User's choice:** All audio-related features, no text.
**Notes:** Duplicate key is `(original_audio_uri, offset, duration)`.

| Option | Description | Selected |
|--------|-------------|----------|
| Exact normalized string match only | Strip whitespace and compare exact URI strings. | yes |
| Canonicalize obvious URI variants | Normalize URL encoding or signed-query variants. | |
| Require explicit alias map | Exact match by default, sidecar map for aliases. | |

**User's choice:** Exact normalized string match only.
**Notes:** Keep implementation simple and predictable.

---

## Balance Reports

| Option | Description | Selected |
|--------|-------------|----------|
| Core factors only | Dataset name/family, row count, duration, source count. | |
| All listed factors | Include time/month/hour and all buckets. | |
| Two-tier weighted scoring | High weight for core factors, lower weight for secondary factors. | |
| Performance-correlated factors | Dataset name/family, rows, duration, source count, duration buckets, transcript-length buckets. | yes |

**User's choice:** Score anything that can impact transcription performance.
**Notes:** Train and eval should have similar duration-bucket and transcript-length-bucket distributions.

| Option | Description | Selected |
|--------|-------------|----------|
| Report only unless available | Do not require or score timestamps. | yes |
| Score when available | Include month/hour where present. | |
| Require timestamps | Block timestamp-capable datasets without timestamp fields. | |

**User's choice:** Report only.
**Notes:** Timestamp/month/hour balance must not drive splitting.

| Option | Description | Selected |
|--------|-------------|----------|
| Fixed built-in buckets | Simple and consistent across runs. | yes |
| Configurable buckets in TOML | Flexible but expands config. | |
| Auto-quantile buckets per dataset | Adaptive but harder to compare across versions. | |

**User's choice:** Fixed built-in buckets.
**Notes:** Applies to duration and transcript-length buckets.

| Option | Description | Selected |
|--------|-------------|----------|
| Word count | Proxy for ASR target complexity. | yes |
| Character count | Simpler and language-agnostic. | |
| Both word and character buckets | More diagnostics. | |

**User's choice:** Word count.
**Notes:** Use normalized transcript word count.

| Option | Description | Selected |
|--------|-------------|----------|
| Fail only hard gates; report balance deltas | Balance is not a hard gate. | yes |
| Fail if balance exceeds thresholds | Enforce maximum deltas. | |
| Warn if balance exceeds thresholds | Continue but mark poor balance. | |

**User's choice:** Fail only hard gates.
**Notes:** Hard failures are leakage, duplicates, zero-valid dataset, and impossible split conditions.

| Option | Description | Selected |
|--------|-------------|----------|
| Weighted balance score plus component deltas | One score plus explainable components. | yes |
| Component deltas only | No single score. | |
| Pass/warn labels only | Minimal report. | |

**User's choice:** Weighted balance score plus component deltas.
**Notes:** Chosen split should be auditable.

---

## the agent's Discretion

- Choose the concrete one-implementation balance algorithm after evaluating practical options.
- Choose fixed duration and transcript-word-count bucket boundaries.

## Deferred Ideas

- GCS artifact writing and model-specific manifests remain Phase 3.
- Audio clipping/derivation and provenance completion remain Phase 4.
- Training execution remains v2.
