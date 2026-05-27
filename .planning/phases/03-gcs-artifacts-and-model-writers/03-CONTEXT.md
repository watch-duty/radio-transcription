# Phase 3: GCS Artifacts And Model Writers - Context

**Gathered:** 2026-05-27
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 3 writes immutable dataset-version artifacts to GCS after Phase 2 has produced leak-safe train/SFT Eval Split assignments. It owns the GCS layout, canonical train/eval manifests, per-dataset slices, model-input writers for NeMo, Whisper, and Gemini, and dataset-version generation reports.

It does not submit SFT jobs, compare SFT runs, derive audio clips, or modify existing benchmark/eval manifests.

</domain>

<decisions>
## Implementation Decisions

### Artifact Layout And Overwrite Safety
- **D-01:** Use `gs://wd-transcription-data/sft/{dataset_version_id}/` as the dataset-version root.
- **D-02:** Organize artifacts under `config/`, `metadata/`, `manifests/`, `model_inputs/`, `reports/`, and a reserved `audio/` area for Phase 4.
- **D-03:** SFT run reports are not dataset-version reports. Future tuning run metrics, tuned model IDs, post-run eval, and run comparisons must live outside the immutable dataset-version tree.
- **D-04:** Any object under the dataset-version prefix means the dataset version already exists.
- **D-05:** Phase 3 has no force mode. If any object exists under the prefix, generation fails. No overwrite, partial resume, or `--force` flag.

### Canonical Manifest Schema
- **D-06:** Canonical manifests are enriched audit rows, one JSONL object per SFT example.
- **D-07:** Rows must include source/split/provenance fields such as `dataset_name`, `dataset_family`, `source_group`, `split`, `audio_uri`, `original_audio_uri`, `text`, `offset`, `duration`, stable IDs, optional timestamp, and optional model-ready/derived URI fields.
- **D-08:** Do not include `raw_row` in generated canonical manifests.
- **D-09:** Phase 3 uses original source audio URIs directly and preserves `offset`/`duration`. Phase 4 owns clip derivation or model-ready URI adjustment.
- **D-10:** Do not mark Phase 3 artifacts as draft/pre-derivation or `requires_audio_derivation`; Phase 4 may adjust if needed.

### Model Writer Outputs
- **D-11:** NeMo writer emits standard train/eval JSONL with `audio_filepath`, `text`, `duration`, and `offset` when pointing at original longer audio spans, plus a config fragment pointing at train and validation/eval manifests.
- **D-12:** Whisper writer emits loader-friendly JSONL with audio URI/path, transcript, duration, source/split metadata, and preprocessing recommendations.
- **D-13:** Whisper examples over 30 seconds are report warnings unless a verified consumer rejects them.
- **D-14:** Gemini writer emits Vertex/Gemini SFT JSONL using the existing `common.sft.build_example`/`validate_example` shape.
- **D-15:** Gemini writer defaults to configurable Gemini 3.1 Flash-Lite based on the user-provided current Gemini Enterprise Agent Platform documentation, while keeping `base_model`, region, adapter size, epochs, and learning-rate multiplier configurable.
- **D-16:** Gemini config must treat `trainingDatasetUri` as required and `validationDatasetUri` as optional.

### Reports And Validation Policy
- **D-17:** Write `metadata/dataset_version.json` plus `reports/dataset_version_report.json` and `reports/dataset_version_report.md`.
- **D-18:** Reports must include config copy or resolved config, split counts, duration/counts by split/dataset/model writer, leakage validation result, balance score/components, artifact URI inventory, and writer validation warnings.
- **D-19:** Dataset-version reports are about dataset generation only. They must not contain SFT run metrics or tuned-model results.
- **D-20:** Hard fail structural/data-shape errors and known target rejections. Report target-specific performance risks. This follows current eval/SFT behavior: malformed merge keys and Gemini SFT preflight failures are blockers, while runtime/performance risks are surfaced without globally blocking unrelated artifacts.

### The Agent's Discretion
- Choose exact helper/module names, local staging locations, report JSON field grouping, and small abstractions as long as the public artifact layout and validation policy above remain stable.
- Prefer existing project patterns over new framework-level interfaces. Phase 3 only needs one implementation path.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project And Prior Phase Context
- `.planning/PROJECT.md` - Defines the milestone objective and dataset-version terminology.
- `.planning/REQUIREMENTS.md` - Lists artifact, model writer, and verification requirements.
- `.planning/ROADMAP.md` - Defines Phase 3 scope and plan names.
- `.planning/STATE.md` - Current project state and active focus.
- `.planning/phases/01-manifest-and-source-identity/01-CONTEXT.md` - Source Group and labeled-segment decisions.
- `.planning/phases/02-split-engine-and-leakage-gates/02-CONTEXT.md` - Split, leakage, and balance decisions that Phase 3 consumes.

### Split And Dataset Internals
- `model/scripts/sft/dataset_split/types.py` - `LabeledSegment` internal shape, including source/provenance/split fields.
- `model/scripts/sft/dataset_split/split.py` - Source-group assignment and split metadata.
- `model/scripts/sft/dataset_split/leakage.py` - Hard leakage gates.
- `model/scripts/sft/dataset_split/balance.py` - Balance report structure and component scoring.
- `model/scripts/sft/dataset_split/gcs_io.py` - Strict GCS manifest reading/parsing behavior.

### Existing Eval And SFT Contracts
- `model/colabs/common/manifest.py` - Existing eval/model-facing `CanonicalRow`, manifest loading, and fail-loud prediction merge behavior.
- `model/colabs/common/scoring.py` - Existing duration bucket behavior for eval analysis.
- `model/colabs/common/inference_hf.py` - Whisper/HF-style eval handling for per-row runtime failures.
- `model/colabs/common/inference_nemo.py` - NeMo eval handling for per-row runtime failures.
- `model/colabs/common/sft.py` - Gemini SFT example builder and validator.
- `model/colabs/common/gcs_utils.py` - GCS URI parsing, exists checks, download/upload helpers.
- `model/colabs/common/vertex.py` - Gemini tuning/batch helpers and current configurable tuning parameters.
- `model/scripts/sft/pipeline.py` - Existing Gemini SFT build path and legacy `round_id` behavior.
- `model/scripts/sft/preflight.py` - Hard-gated Gemini SFT preflight behavior.
- `model/scripts/sft/adapters/gcs_manifest.py` - Existing adapter from GCS JSONL into eval/SFT rows.

### External API Documentation
- Context7 `/googleapis/python-storage` - GCS blob existence and upload overwrite/precondition behavior.
- Context7 `/websites/cloud_google_vertex-ai` - Vertex/Gemini supervised tuning fields, including training/validation dataset URIs and hyperparameters.
- `https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-use-supervised-tuning` - User-provided current Gemini Enterprise Agent Platform supervised tuning model support, including Gemini 3.1 Flash-Lite.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `LabeledSegment`: Use as the internal source of truth for generated canonical rows.
- `validate_split_integrity`: Run before writing artifacts so Phase 3 does not publish leaking splits.
- `BalanceReport`: Include its score/components in the dataset-version report.
- `common.sft.build_example` and `validate_example`: Reuse for Gemini writer output.
- `common.gcs_utils.parse_gcs_uri`, `blob_exists`, and upload helpers: Reuse for GCS path handling; add no-overwrite behavior where needed.

### Established Patterns
- Existing eval uses `audio_filepath`, `offset`, `duration`, and `text` as the stable model/eval row fields.
- Existing prediction merge fails loud on malformed join keys because silent defaults corrupt metrics.
- Existing HF/Whisper and NeMo eval paths surface per-audio runtime failures without invalidating the entire run.
- Existing Gemini SFT preflight hard fails malformed SFT examples, duplicate/overlapping file URIs, unreachable file URIs, and token-limit violations.

### Integration Points
- New Phase 3 code should connect after Phase 2 split assignment and before Phase 4 audio derivation.
- Generated model inputs should be consumable by existing or future NeMo, Whisper/HF, and Gemini SFT workflows without modifying benchmark/eval manifests.
- Keep generated/resolved state and reports as JSON/JSONL; input config remains TOML from earlier decisions.

</code_context>

<specifics>
## Specific Ideas

Suggested dataset-version tree:

```text
gs://wd-transcription-data/sft/{dataset_version_id}/
  config/
  metadata/
  manifests/
    canonical/
    per_dataset/
  model_inputs/
    nemo/
    whisper/
    gemini/
  reports/
  audio/
```

`reports/` in this tree means dataset-version generation reports only. Future SFT run reports should use a separate run-specific location.

</specifics>

<deferred>
## Deferred Ideas

- SFT run reports, tuned model IDs, tuning metrics, post-run eval, and run comparison reports belong to a later run-specific workflow.
- Audio derivation, clip upload, transformation provenance, and model-ready URI replacement belong to Phase 4.
- Force/overwrite, partial resume, and prefix cleanup are out of Phase 3.
- Tarred/sharded large-dataset artifacts remain a future scaling concern.

</deferred>

---

*Phase: 3-GCS Artifacts And Model Writers*
*Context gathered: 2026-05-27*
