# Phase 1: Manifest And Source Identity - Context

**Gathered:** 2026-05-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 1 defines and validates the input side of SFT dataset versioning. It introduces the dataset-version job config, loads configured GCS manifests, normalizes manifest rows into an internal source-aware shape, and resolves leak-safe Source Groups for Broadcastify Calls, Broadcastify Feeds, Echo, and Fire Notifications. It does not generate the 80:20 split, write model-specific artifacts, derive audio clips, or run tuning jobs.

</domain>

<decisions>
## Implementation Decisions

### Input Registry Contract
- **D-01:** Introduce a new dataset-version config, separate from the existing Gemini-oriented `model/scripts/sft/datasets.toml`.
- **D-02:** Use TOML for the user-authored dataset-version input config. Generated/resolved state and reports should use JSON/JSONL.
- **D-03:** Require explicit `source_strategy` in every dataset entry so the leak-prevention method is visible in review.
- **D-04:** User-facing manifest and sidecar source-map inputs must be `gs://` URIs. Local fixture files are allowed only behind fake/mocked readers in tests.
- **D-05:** Production loader failures should fail fast with structured context: dataset name, manifest URI, source strategy, row index when available, reason, and relevant URI/source fields.

### Normalized Row Shape
- **D-06:** Add a richer internal `LabeledSegment` or `SourceTaggedRow` type for dataset-versioning internals. Preserve `CanonicalRow` as the existing model/eval boundary row.
- **D-07:** The internal validation object may retain `raw_row` while validating. Generated model artifacts must not include raw rows.
- **D-08:** Failure diagnostics may include safe, relevant raw keys/values when useful, but should primarily report normalized context such as dataset name, manifest URI, row index, audio URI, and missing/ambiguous source fields.
- **D-09:** Define the full `LabeledSegment` schema early, including future writer/provenance fields. Phase 1 only requires fields needed for source identity and manifest validity; later phases may populate nullable fields such as derived clip URI, transformation metadata, split, and model-ready URI.

### Source-Key Fallback Policy
- **D-10:** Each `source_strategy` has a fixed, tested extractor cascade. Per-dataset configurable fallback order is out of scope for Phase 1.
- **D-11:** Echo source identity requires both `area_code` and `echo_name`, whether explicit or inferred.
- **D-12:** Use the existing repo Echo mono-stream registry snapshot as the default Echo disambiguation source. A configured `gs://` source map may override/replace it later, but Phase 1 must not require new user-provided metadata.
- **D-13:** If Echo cannot resolve a row through explicit fields, URI/S3 key parsing, configured source map, or the built-in registry, it fails source-key validation rather than guessing.
- **D-14:** Broadcastify Calls source groups are `bcfy_calls:<groupId>`. Broadcastify Feeds source groups are `bcfy_feeds:<feedId>`.
- **D-15:** Broadcastify source extractors use explicit source fields first, with URI/filename fallback only through family-specific parsers. If a row only has an archive/CDN URL that does not encode the required ID, fail rather than guess.
- **D-16:** Fire Notification source groups use stream path/location, not the collection day UUID.

### Validation And Reporting UX
- **D-17:** Hard validation failures fail fast with a short contextual error. Do not build a full row-level cleanup report in Phase 1.
- **D-18:** Empty or missing normalized text is a soft exclusion with counts, matching existing Gemini/Chirp eval and SFT behavior.
- **D-19:** A dataset that produces zero valid examples after soft exclusions is a hard failure.
- **D-20:** Phase 1 surfaces validation and exclusion information through CLI/log summary only: loaded rows, valid rows, and empty-text exclusions per dataset.

### the agent's Discretion
No broad "you decide" areas were granted. The agent may choose implementation names such as `LabeledSegment` versus `SourceTaggedRow` if the choice is consistent with repository naming and test clarity.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope And Requirements
- `.planning/PROJECT.md` — Project core value, glossary, constraints, prior validated dataset observations, and source-group decisions.
- `.planning/REQUIREMENTS.md` — Phase 1 requirements and traceability; note that `INPT-03` is updated by this discussion to be GCS-only for user-facing configs.
- `.planning/ROADMAP.md` — Phase boundaries, success criteria, and plan breakdown.
- `.planning/STATE.md` — Current project state and recent decision summary.

### Codebase Maps
- `.planning/codebase/ARCHITECTURE.md` — Offline model/SFT tooling integration points and system context.
- `.planning/codebase/INTEGRATIONS.md` — GCS, Vertex, and ingestion/eval integration context.
- `.planning/codebase/CONVENTIONS.md` — Python/test/style patterns to follow.

### Existing Manifest And SFT Code
- `model/colabs/common/manifest.py` — Existing `CanonicalRow`, manifest loading, row conversion, and prediction merge behavior.
- `model/colabs/common/sft.py` — Existing Gemini SFT example builder and schema validator; rejects empty model target text.
- `model/scripts/sft/pipeline.py` — Existing Gemini-oriented SFT pipeline and `datasets.toml` usage; new dataset-version config should remain separate.
- `model/scripts/sft/adapters/gcs_manifest.py` — Existing GCS manifest adapter pattern for model-facing rows.
- `model/scripts/sft/preflight.py` — Existing preflight validation behavior for generated Gemini SFT JSONL.

### Eval Behavior Checked During Discussion
- `model/colabs/gemini_create_inference_manifest.ipynb` — Gemini eval merge behavior; unmatched predictions become empty prediction text and baseline rows are preserved.
- `model/colabs/chirp_create_inference_manifest.ipynb` — Chirp eval merge behavior; ground-truth text defaults to empty string when absent.
- `model/colabs/evaluate_transcriptions.ipynb` — Scoring behavior; rows with empty normalized ground truth are skipped.
- `model/colabs/common/scoring.py` — Shared scoring helpers; duration bucket scoring excludes empty references.

### Echo Source Identity
- `model/data_sources/echo/README.md` — Documents the Echo mono-stream CSV snapshot and S3 key format.
- `model/data_sources/echo/all_echo_mono_streams.csv` — Built-in Echo registry snapshot used for Phase 1 default disambiguation.
- `model/data_sources/echo/s3_file_scanner.py` — Echo S3 path shape: `<device_name>/<date>/<filename_prefix>_<date>_<hour>.mp3`.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `model/colabs/common/manifest.py`: Keep `CanonicalRow` stable for existing eval/SFT consumers; add richer dataset-versioning types elsewhere or alongside without breaking current tests.
- `model/scripts/sft/adapters/gcs_manifest.py`: Reuse the GCS-manifest reading pattern where practical, but Phase 1 needs stricter failure behavior than existing soft local `load_manifest`.
- `model/data_sources/echo/all_echo_mono_streams.csv`: Use as a default Echo registry source. It proves that `echo_name` alone is unsafe because names such as `Tehama_Sheriff_Disp` appear under multiple area codes.

### Established Patterns
- Existing eval paths tolerate missing/empty ground-truth text and skip it at scoring time.
- Existing Gemini SFT validation rejects empty target text, and the build path skips invalid examples.
- Existing model/SFT code is lightweight Python with focused pytest tests under nearby `tests/` directories.

### Integration Points
- New Phase 1 code should live in model/SFT tooling boundaries, likely under `model/scripts/sft` or a nearby dataset-versioning module, and should not modify historical benchmark/eval manifests.
- Later phases will convert valid `LabeledSegment` rows into canonical train/eval JSONL and model-specific writers for NeMo, Whisper, and Gemini.

</code_context>

<specifics>
## Specific Ideas

- A dataset-version TOML should support one job with `dataset_version_id`, seed, train/eval ratio, output GCS prefix, and one or more `[[datasets]]` entries.
- Each dataset entry should include `name`, `family`, `manifest_uri`, `source_strategy`, and optional `source_map_uri`.
- `source_strategy` should be explicit even when it matches `family`.
- Public configs should reject non-`gs://` manifest/source-map URIs.
- `Tehama_Sheriff_Disp` is not safe as an Echo source key because it appears under both `ca_chico` and `ca_red_bluff`.

</specifics>

<deferred>
## Deferred Ideas

- Full JSON/Markdown report bundles belong in later reporting/artifact phases, not Phase 1.
- Split balancing, leakage gates, GCS artifact writing, model-specific writer output, and audio derivation remain in Phases 2-4.

</deferred>

---

*Phase: 1-Manifest And Source Identity*
*Context gathered: 2026-05-27*
