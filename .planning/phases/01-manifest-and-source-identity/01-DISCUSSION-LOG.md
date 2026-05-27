# Phase 1: Manifest And Source Identity - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-27
**Phase:** 1-Manifest And Source Identity
**Areas discussed:** Input Registry Contract, Normalized Row Shape, Source-Key Fallback Policy, Validation And Reporting UX

---

## Input Registry Contract

| Question | Options Considered | User's Choice |
|----------|--------------------|---------------|
| What should the user-facing dataset-version input config be? | New dataset-version config; extend existing `datasets.toml`; CLI-only arguments plus sidecars | New dataset-version config |
| Which file format should we implement first? | TOML; YAML; JSON | TOML for authored config; JSON/JSONL for generated/resolved outputs |
| Should source-key extraction strategy be explicit in each dataset entry? | Required explicit strategy; default from family with override; infer from row fields/URI | Require explicit `source_strategy` |
| What URI/path types should Phase 1 support for manifests and sidecar source maps? | Local+GCS; local+GCS+S3+HTTPS; GCS-only | GCS-only for user-facing config |
| Should `INPT-03` remove local-disk support from the user-facing contract? | Yes; keep requirement but implement later; keep local+GCS | Remove local-disk support, while tests cover failure classes through fake/mocked GCS readers |

**Notes:** The user questioned why JSON could not handle repeated dataset entries. We clarified that JSON can represent the structure, but TOML is better for hand-authored, reviewable config with comments. The final split is TOML for human intent and JSON/JSONL for machine outputs.

---

## Normalized Row Shape

| Question | Options Considered | User's Choice |
|----------|--------------------|---------------|
| Extend `CanonicalRow`, or introduce a separate internal row type? | Separate `LabeledSegment`/`SourceTaggedRow`; extend `CanonicalRow`; plain dicts | Separate richer internal type, preserve `CanonicalRow` at model/eval boundary |
| Should the internal row retain the full raw manifest row? | Keep `raw_row`; normalized fields only; selected raw fields only | `raw_row` may exist internally; generated artifacts must not include it |
| Should source extraction failures be represented as invalid row objects, or raised immediately? | Collect row-level validation results then fail; raise immediately; continue silently | Initially collect results, later revised to fail fast in validation UX |
| Minimum required normalized fields for a valid `LabeledSegment` after Phase 1? | Core split fields only; core plus future writer/provenance fields; `CanonicalRow` plus source group | Define full schema early, but only Phase 1 fields are required now |

**Notes:** The user asked whether existing eval and SFT should share the same row type. We clarified that sharing is useful at the boundary, but leak-safe splitting needs more metadata than `CanonicalRow` can carry.

---

## Source-Key Fallback Policy

| Question | Options Considered | User's Choice |
|----------|--------------------|---------------|
| Fixed extractor cascade or configurable cascade? | Fixed per `source_strategy`; configurable per dataset; one generic cascade | Fixed per `source_strategy` |
| For Echo, require both `area_code` and `echo_name`? | Require both; allow globally unique `echo_name`; allow URI path alone | Require both |
| Should Phase 1 include a canonical Echo device registry/source map input? | Require/configure source map; rely only on fields/URI; bake known CSV into code/tests | Use existing repo Echo registry snapshot by default; no new metadata required from user |
| For Broadcastify Calls/Feeds, accept parsed source IDs or require explicit fields? | Explicit fields first, family parser fallback; explicit only; parse first | Explicit first, parser fallback only when family-specific and unambiguous |

**Notes:** We validated the Echo assumption against actual repo data. `Tehama_Sheriff_Disp` appears under both `ca_chico` and `ca_red_bluff`, and the Echo registry snapshot has many duplicated echo names, so `echo_name` alone is unsafe.

---

## Validation And Reporting UX

| Question | Options Considered | User's Choice |
|----------|--------------------|---------------|
| What should the user get on hard validation failures? | Nonzero exit plus structured report path; exception traceback; warnings only | Fail fast with short contextual error |
| Collect all invalid rows before failing, or fail on first hard validation failure? | Fail first; collect compact summary | Fail on first hard validation failure |
| Should empty/missing normalized text be hard failure or soft exclusion? | Soft exclusion with counts; hard failure | Soft exclusion with counts |
| Where should Phase 1 surface exclusion counts and validation results? | CLI/log summary only; small JSON summary; both | CLI/log summary only |

**Notes:** Before deciding empty-text behavior, we checked existing Gemini and Chirp eval behavior. Existing eval merges preserve baseline rows, scoring skips empty normalized ground truth, and existing SFT validation rejects empty model target text. The Phase 1 choice matches that behavior.

---

## the agent's Discretion

- Choose the concrete internal row type name if it follows repository conventions and keeps tests clear.

## Deferred Ideas

- Full JSON/Markdown report bundles are deferred to later artifact/report phases.
