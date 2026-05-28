# Phase 3: GCS Artifacts And Model Writers - Research

**Researched:** 2026-05-27 [VERIFIED: system date]
**Domain:** Python dataset-version artifact generation, Google Cloud Storage create-only writes, and ASR model input JSONL writers [VERIFIED: .planning/phases/03-gcs-artifacts-and-model-writers/03-CONTEXT.md]
**Confidence:** HIGH for GCS/artifact contracts and existing repo integration; MEDIUM for future Gemini model defaults because Agent Platform model support changes quickly [VERIFIED: Context7 /googleapis/python-storage; CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning]

<user_constraints>
## User Constraints (from CONTEXT.md)

All copied constraints in this block are sourced from `.planning/phases/03-gcs-artifacts-and-model-writers/03-CONTEXT.md`. [VERIFIED: .planning/phases/03-gcs-artifacts-and-model-writers/03-CONTEXT.md]

### Locked Decisions

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

### the agent's Discretion
- Choose exact helper/module names, local staging locations, report JSON field grouping, and small abstractions as long as the public artifact layout and validation policy above remain stable.
- Prefer existing project patterns over new framework-level interfaces. Phase 3 only needs one implementation path.

### Deferred Ideas (OUT OF SCOPE)

- SFT run reports, tuned model IDs, tuning metrics, post-run eval, and run comparison reports belong to a later run-specific workflow.
- Audio derivation, clip upload, transformation provenance, and model-ready URI replacement belong to Phase 4.
- Force/overwrite, partial resume, and prefix cleanup are out of Phase 3.
- Tarred/sharded large-dataset artifacts remain a future scaling concern.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ARTF-01 | Write one dataset version under the locked GCS root. [VERIFIED: .planning/REQUIREMENTS.md] | Use a layout planner rooted at `gs://wd-transcription-data/sft/{dataset_version_id}/`. [VERIFIED: 03-CONTEXT.md] |
| ARTF-02 | Protect existing dataset-version paths. [VERIFIED: .planning/REQUIREMENTS.md] | Check `Client.list_blobs(prefix=..., max_results=1)` before any upload and use `if_generation_match=0` per object. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration] |
| ARTF-03 | Write canonical train/eval JSONL manifests. [VERIFIED: .planning/REQUIREMENTS.md] | Serialize `LabeledSegment` rows after `validate_split_integrity()` and omit `raw_row`. [VERIFIED: model/scripts/sft/dataset_split/types.py; VERIFIED: model/scripts/sft/dataset_split/leakage.py; VERIFIED: 03-CONTEXT.md] |
| ARTF-04 | Write per-dataset train/eval JSONL slices. [VERIFIED: .planning/REQUIREMENTS.md] | Group assigned segments by `dataset_name` and `split` without recomputing the split. [VERIFIED: model/scripts/sft/dataset_split/split.py] |
| ARTF-05 | Write JSON reports and a Markdown summary. [VERIFIED: .planning/REQUIREMENTS.md] | Include config, leakage, balance, writer warnings, and artifact inventory. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/scripts/sft/dataset_split/balance.py] |
| ARTF-06 | Keep generated manifests/audio out of Git. [VERIFIED: .planning/REQUIREMENTS.md] | Artifact generation writes to GCS or local staging only; commit only code/tests/planning docs. [VERIFIED: AGENTS.md; VERIFIED: .planning/PROJECT.md] |
| MODL-01 | NeMo writer emits `audio_filepath`, `text`, and `duration`. [VERIFIED: .planning/REQUIREMENTS.md] | Reuse the existing eval row field convention and include `offset` for longer source audio spans. [VERIFIED: model/colabs/common/manifest.py; VERIFIED: 03-CONTEXT.md] |
| MODL-02 | NeMo writer emits a config fragment. [VERIFIED: .planning/REQUIREMENTS.md] | Write a small YAML/JSON fragment with train and validation manifest URIs, not a full training job spec. [VERIFIED: 03-CONTEXT.md] |
| MODL-03 | Whisper writer emits loader-friendly manifests with metadata. [VERIFIED: .planning/REQUIREMENTS.md] | Preserve audio URI/path, text, duration, source metadata, split, and preprocessing recommendations. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/colabs/common/inference_hf.py] |
| MODL-04 | Whisper records or enforces the sub-30-second constraint. [VERIFIED: .planning/REQUIREMENTS.md] | Follow D-13: warn on examples over 30 seconds unless a verified consumer rejects them. [VERIFIED: 03-CONTEXT.md] |
| MODL-05 | Gemini writer emits Vertex SFT JSONL with `systemInstruction`, `contents`, `fileData`, `mimeType`, and target transcript text. [VERIFIED: .planning/REQUIREMENTS.md] | Use the existing `common.sft` shape and update MIME handling for original MP3/FLAC sources. [VERIFIED: model/colabs/common/sft.py; CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning-prepare; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune] |
| MODL-06 | Gemini writer emits tuning config fields. [VERIFIED: .planning/REQUIREMENTS.md] | Emit `trainingDatasetUri`, optional `validationDatasetUri`, `baseModel`, region, adapter size, epoch count, and learning-rate multiplier. [CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-use-supervised-tuning; VERIFIED: model/colabs/common/vertex.py] |
| MODL-07 | Gemini writer supports Gemini 3.1 Flash-Lite while keeping base model configurable. [VERIFIED: .planning/REQUIREMENTS.md] | Agent Platform docs list Gemini 3.1 Flash-Lite as supervised-fine-tuning capable. [CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning] |
| MODL-08 | Existing benchmark/eval manifests remain unchanged. [VERIFIED: .planning/REQUIREMENTS.md] | Generate new dataset-version artifacts only; do not mutate `model/colabs/common/manifest.py` consumers or historical manifests. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/colabs/common/manifest.py] |
| TEST-05 | Tests cover model-writer output shapes. [VERIFIED: .planning/REQUIREMENTS.md] | Add writer-shape tests under `model/scripts/sft/tests/`. [VERIFIED: .planning/codebase/TESTING.md] |
| TEST-06 | Tests cover existing-path protection. [VERIFIED: .planning/REQUIREMENTS.md] | Add fake-client tests for prefix existence and per-object precondition failures. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client; VERIFIED: backend/pipeline/common/storage/tests/test_gcs_uploader.py] |
</phase_requirements>

## Summary

Phase 3 should be planned as a deterministic artifact-publishing layer that consumes already-assigned Phase 2 `LabeledSegment` rows and writes a new immutable dataset-version tree. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/scripts/sft/dataset_split/types.py] It should not resplit, derive clips, submit tuning jobs, or write SFT run metrics. [VERIFIED: 03-CONTEXT.md; VERIFIED: .planning/ROADMAP.md]

The safest implementation is a two-gate GCS strategy: first reject the dataset version if any object exists under the prefix, then use create-only upload preconditions for every object. [VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration] This is stricter than the existing async ingestion helper that treats HTTP 412 as idempotent success; Phase 3 should fail on 412 because partial resume and overwrite are out of scope. [VERIFIED: backend/pipeline/common/gcp_helper.py; VERIFIED: 03-CONTEXT.md]

The biggest planning risk is Gemini audio MIME handling: existing `common.sft.build_example()` hardcodes `audio/flac`, but Phase 3 uses original source URIs and current Google audio-tuning docs show an MP3 example with `audio/mpeg`. [VERIFIED: model/colabs/common/sft.py; VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune] Plan a targeted update to `common.sft` so the writer can emit truthful `fileData.mimeType` for verified formats while preserving the existing nested JSONL shape. [VERIFIED: model/colabs/common/tests/test_sft.py; CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning-prepare]

**Primary recommendation:** Implement a small artifact module set under `model/scripts/sft/dataset_split/` that validates Phase 2 output, builds all JSON/JSONL artifacts locally/in memory, performs one prefix-existence check, then uploads every artifact with create-only GCS preconditions. [VERIFIED: model/scripts/sft/dataset_split; VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/storage/docs/samples/storage-upload-file]

## Project Constraints (from AGENTS.md)

- Generated dataset artifacts and derived audio belong in GCS, not Git. [VERIFIED: AGENTS.md]
- Generated model inputs must match current NeMo, Whisper, and Gemini/Vertex requirements verified from current docs during implementation. [VERIFIED: AGENTS.md]
- Dataset artifacts and derived clips are scoped under `gs://wd-transcription-data/sft/{dataset_version_id}/`. [VERIFIED: AGENTS.md]
- Use the `ctx7` CLI for library, framework, SDK, API, CLI, and cloud-service documentation questions; use a provided `/org/project` library ID directly when available. [VERIFIED: user-provided AGENTS.md instructions]
- Do not put sensitive values such as API keys, passwords, or credentials into documentation queries. [VERIFIED: user-provided AGENTS.md instructions]
- Python formatting is Ruff-managed with `line-length = 80`, and model/SFT scripts have a relaxed Ruff profile under the root `pyproject.toml`. [VERIFIED: pyproject.toml; VERIFIED: AGENTS.md]
- Model/SFT CLI code and tests belong under `model/scripts/sft/`, with existing tests under `model/scripts/sft/tests/`. [VERIFIED: AGENTS.md; VERIFIED: .planning/codebase/TESTING.md]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Dataset-version layout planning | Offline model tooling | GCS | The `model/scripts/sft` code constructs artifact paths; GCS only stores objects. [VERIFIED: .planning/ROADMAP.md; VERIFIED: model/scripts/sft/pipeline.py] |
| Prefix existence protection | Offline model tooling | GCS | Code must query GCS before writing and reject if any object already exists. [VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client] |
| Canonical/per-dataset manifests | Offline model tooling | GCS | `LabeledSegment` rows already carry split/source/provenance fields; GCS stores emitted JSONL. [VERIFIED: model/scripts/sft/dataset_split/types.py] |
| Model input writers | Offline model tooling | Existing eval/SFT helpers | Writer shapes should follow existing `common.manifest`, `common.sft`, and inference helper contracts. [VERIFIED: model/colabs/common/manifest.py; VERIFIED: model/colabs/common/sft.py; VERIFIED: model/colabs/common/inference_hf.py; VERIFIED: model/colabs/common/inference_nemo.py] |
| Dataset-version reports | Offline model tooling | GCS | Reports summarize generation artifacts, leakage validation, balance, and writer warnings. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/scripts/sft/dataset_split/balance.py] |
| Tuning job submission | Out of scope | Vertex/Gemini | Phase 3 emits inputs/configs only; tuning execution is deferred. [VERIFIED: .planning/ROADMAP.md; VERIFIED: 03-CONTEXT.md] |

## Standard Stack

### Core

| Library / Module | Version | Purpose | Why Standard |
|------------------|---------|---------|--------------|
| Python | Project target 3.13.2; `uv run --project model` used CPython 3.13.12 in this session. [VERIFIED: .tool-versions; VERIFIED: uv run --project model] | Runs `model/scripts/sft` and `model/colabs/common` tooling. [VERIFIED: model/pyproject.toml] | The repo pins Python 3.13.x for backend/model work. [VERIFIED: .tool-versions; VERIFIED: pyproject.toml] |
| `google-cloud-storage` | Model env resolved 3.10.1; root lock has 2.19.0; model requirement is `>=2.19`. [VERIFIED: uv run --project model; VERIFIED: uv.lock; VERIFIED: model/pyproject.toml] | GCS prefix checks and create-only artifact uploads. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client] | Official client supports `list_blobs(prefix=...)` and `if_generation_match=0`. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration] |
| `model/scripts/sft/dataset_split` | Local package, no published version. [VERIFIED: model/scripts/sft/dataset_split] | Existing config, validation, split assignment, leakage, and balance primitives. [VERIFIED: model/scripts/sft/dataset_split/config.py; VERIFIED: model/scripts/sft/dataset_split/split.py; VERIFIED: model/scripts/sft/dataset_split/leakage.py; VERIFIED: model/scripts/sft/dataset_split/balance.py] | It already owns the Phase 1 and Phase 2 dataset-version internals. [VERIFIED: .planning/phases/01-manifest-and-source-identity/01-RESEARCH.md; VERIFIED: .planning/phases/02-split-engine-and-leakage-gates/02-RESEARCH.md] |
| `model/colabs/common` | Local package `common` 0.1.0. [VERIFIED: model/pyproject.toml] | Existing eval/SFT row contracts and Gemini helpers. [VERIFIED: model/colabs/common/manifest.py; VERIFIED: model/colabs/common/sft.py; VERIFIED: model/colabs/common/vertex.py] | Phase 3 must emit model inputs that match current repo consumers. [VERIFIED: 03-CONTEXT.md] |

### Supporting

| Library / Module | Version | Purpose | When to Use |
|------------------|---------|---------|-------------|
| `google-genai` | Model env resolved 2.6.0 from `google-genai>=2.3,<3`. [VERIFIED: uv run --project model --extra vertex; VERIFIED: model/pyproject.toml] | Existing `common.vertex` tuning submission helper and future config parity. [VERIFIED: model/colabs/common/vertex.py] | Do not call tuning APIs in Phase 3; only mirror config field names for later consumers. [VERIFIED: 03-CONTEXT.md] |
| `ortools` | Model env resolved 9.15.6755 from `ortools>=9.15,<10`. [VERIFIED: uv run --project model --extra optimizer; VERIFIED: model/pyproject.toml] | Phase 2 split assignment dependency. [VERIFIED: model/scripts/sft/dataset_split/split.py] | Consume its saved `SplitResult`; do not re-solve in Phase 3. [VERIFIED: model/scripts/sft/dataset_split/split.py; VERIFIED: 03-CONTEXT.md] |
| `pytest` | Model env resolved 9.0.3. [VERIFIED: uv run --project model --extra dev] | Unit tests for artifact and writer contracts. [VERIFIED: model/pyproject.toml; VERIFIED: .planning/codebase/TESTING.md] | Use fake GCS clients for prefix/upload behavior. [VERIFIED: model/scripts/sft/tests; VERIFIED: backend/pipeline/common/storage/tests/test_gcs_uploader.py] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `google-cloud-storage` sync client | Existing async `gcloud-aio-storage` helper | The async helper swallows 412 as idempotent success when preconditions are set, which conflicts with Phase 3 no-resume semantics. [VERIFIED: backend/pipeline/common/gcp_helper.py; VERIFIED: 03-CONTEXT.md] |
| New top-level `dataset_version` package | Extend `model/scripts/sft/dataset_split/` | A new package would be cleaner long-term, but the current Phase 1/2 internals and tests already live under `dataset_split`. [VERIFIED: model/scripts/sft/dataset_split; VERIFIED: .planning/phases/02-split-engine-and-leakage-gates/02-RESEARCH.md] |
| Full Vertex tuning config object | Small JSON config fragment | Phase 3 does not submit jobs, so a small config fragment avoids coupling artifact generation to live Vertex APIs. [VERIFIED: 03-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] |

**Installation / verification commands:** [VERIFIED: shell probes]

```bash
uv run --project model --extra dev python -c "import pytest; print(pytest.__version__)"
uv run --project model --extra vertex python -c "import importlib.metadata as m; print(m.version('google-genai'))"
uv run --project model --extra optimizer python -c "import importlib.metadata as m; print(m.version('ortools'))"
```

## Architecture Patterns

### System Architecture Diagram

```text
DatasetVersionConfig + Phase 2 SplitResult.segments
        |
        v
validate_split_integrity()
        |
        v
ArtifactLayoutPlanner
  - normalize root prefix
  - list_blobs(prefix, max_results=1)
  - fail if any object exists
        |
        v
Artifact Builders
  - canonical train/eval JSONL
  - per-dataset train/eval JSONL
  - NeMo JSONL + config fragment
  - Whisper JSONL + recommendations
  - Gemini SFT JSONL + tuning config
  - metadata + JSON/Markdown reports
        |
        v
Create-only GCS Uploads
  - upload_from_string(..., if_generation_match=0)
  - collect artifact URI inventory
        |
        v
Immutable dataset-version tree in GCS
```

This data flow follows the locked Phase 3 boundary and the current GCS precondition model. [VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration]

### Recommended Project Structure

```text
model/scripts/sft/dataset_split/
├── artifacts.py        # GCS layout, prefix checks, create-only uploads
├── canonical.py        # LabeledSegment -> canonical/per-dataset JSONL rows
├── model_writers.py    # NeMo, Whisper, Gemini writer functions
└── reports.py          # dataset_version metadata, JSON report, Markdown summary

model/scripts/sft/tests/
├── test_dataset_artifacts.py
├── test_dataset_canonical.py
├── test_model_writers.py
└── test_dataset_reports.py
```

This structure keeps Phase 3 close to Phase 1/2 code and avoids changing notebook/eval contracts. [VERIFIED: model/scripts/sft/dataset_split; VERIFIED: model/scripts/sft/tests; VERIFIED: 03-CONTEXT.md]

### Pattern 1: Consume Phase 2 Output As Input

**What:** Accept assigned `LabeledSegment` rows, validate split integrity, then serialize artifacts from those rows. [VERIFIED: model/scripts/sft/dataset_split/types.py; VERIFIED: model/scripts/sft/dataset_split/leakage.py]

**When to use:** Every Phase 3 generation path. [VERIFIED: 03-CONTEXT.md]

**Example:**

```python
from dataset_split.leakage import validate_split_integrity

def build_artifact_bundle(segments: tuple[LabeledSegment, ...]) -> ArtifactBundle:
    validate_split_integrity(segments)
    # Build rows after validation so no leaking dataset version is published.
    return ArtifactBundle(
        canonical_train=canonical_rows(segments, split="train"),
        canonical_eval=canonical_rows(segments, split="eval"),
    )
```

Source: existing Phase 2 validators and Phase 3 boundary. [VERIFIED: model/scripts/sft/dataset_split/leakage.py; VERIFIED: 03-CONTEXT.md]

### Pattern 2: Prefix Guard Before Object Uploads

**What:** Treat any object under the dataset-version prefix as existence, then upload each new object with `if_generation_match=0`. [VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration]

**When to use:** Before writing `config/`, `metadata/`, `manifests/`, `model_inputs/`, `reports/`, or reserved `audio/` placeholders. [VERIFIED: 03-CONTEXT.md]

**Example:**

```python
from common.gcs_utils import parse_gcs_uri
from google.api_core.exceptions import PreconditionFailed

def prefix_exists(client, root_uri: str) -> bool:
    bucket_name, prefix = parse_gcs_uri(root_uri)
    prefix = prefix.rstrip("/") + "/"
    return next(client.list_blobs(bucket_name, prefix=prefix, max_results=1), None) is not None

def upload_text_create_only(client, uri: str, text: str, content_type: str) -> None:
    bucket_name, blob_path = parse_gcs_uri(uri)
    blob = client.bucket(bucket_name).blob(blob_path)
    try:
        blob.upload_from_string(
            text,
            content_type=content_type,
            if_generation_match=0,
        )
    except PreconditionFailed as exc:
        raise DatasetVersionExistsError(uri) from exc
```

Source: current GCS client docs and existing repo create-only upload pattern. [CITED: https://docs.cloud.google.com/storage/docs/samples/storage-upload-file; VERIFIED: backend/pipeline/common/storage/gcs_uploader.py]

### Pattern 3: Writer Warnings Are Data, Not Logs Only

**What:** Writers should return both artifact rows and structured warnings so reports can include per-writer risk summaries. [VERIFIED: 03-CONTEXT.md]

**When to use:** Whisper duration warnings, Gemini MIME/validation warnings, and NeMo offset warnings. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/colabs/common/sft.py]

**Example:**

```python
@dataclass(frozen=True)
class WriterResult:
    rows_by_split: dict[str, list[dict[str, object]]]
    warnings: tuple[dict[str, object], ...]
```

Source: report requirements and existing hard/soft validation split. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/scripts/sft/preflight.py]

### Anti-Patterns to Avoid

- **Calling `assign_train_eval_split()` again:** Phase 3 consumes saved Phase 2 split assignments and must not create a different split. [VERIFIED: model/scripts/sft/dataset_split/split.py; VERIFIED: 03-CONTEXT.md]
- **Using upload helpers that overwrite by default:** GCS uploads can overwrite existing objects unless preconditions are used. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration]
- **Treating 412 as success:** Existing async ingestion does this for idempotent lease writes, but Phase 3 has no partial resume. [VERIFIED: backend/pipeline/common/gcp_helper.py; VERIFIED: 03-CONTEXT.md]
- **Embedding `raw_row` in canonical manifests:** Locked decision D-08 forbids it. [VERIFIED: 03-CONTEXT.md]
- **Writing tuned-model metrics into `reports/`:** Dataset-version reports and SFT run reports are separate. [VERIFIED: 03-CONTEXT.md]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| GCS URI parsing | New parser | `common.gcs_utils.parse_gcs_uri` | Existing helper is used by model tooling and enforces `gs://`. [VERIFIED: model/colabs/common/gcs_utils.py] |
| Split leakage validation | New overlap logic | `validate_split_integrity()` | Existing Phase 2 validator covers source group, original audio URI, model-ready URI, and duplicate spans. [VERIFIED: model/scripts/sft/dataset_split/leakage.py] |
| Gemini SFT shape | Flat prompt/response JSON | `common.sft.build_example` / `validate_example` shape | Existing tests reject legacy flat shapes and current docs use nested `contents`/`fileData`. [VERIFIED: model/colabs/common/tests/test_sft.py; CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning-prepare] |
| GCS no-overwrite | Read-then-write only | `if_generation_match=0` plus prefix preflight | The precondition is the race-safe create-only guard for individual objects. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration] |
| NeMo/eval row contract | New field names | `audio_filepath`, `offset`, `duration`, `text` | Existing eval manifests and merge logic use these fields. [VERIFIED: model/colabs/common/manifest.py] |
| Audio slicing | Inline clip derivation | Phase 4 | Phase 3 must preserve original audio URI plus offset/duration. [VERIFIED: 03-CONTEXT.md] |

**Key insight:** Phase 3 is mostly serialization and publication; correctness depends on preserving Phase 2 invariants and using GCS preconditions, not on inventing new data-processing logic. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/scripts/sft/dataset_split/leakage.py]

## Common Pitfalls

### Pitfall 1: Prefix Check Is Too Narrow

**What goes wrong:** Checking only `metadata/dataset_version.json` misses partial or foreign objects under the dataset-version prefix. [VERIFIED: 03-CONTEXT.md]

**Why it happens:** GCS prefixes are object-name filters, not real directories. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client]

**How to avoid:** Call `list_blobs(bucket, prefix=root_prefix, max_results=1)` and fail if any object is returned. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client]

**Warning signs:** Tests only cover one sentinel object path instead of arbitrary existing objects under `config/`, `manifests/`, or `reports/`. [VERIFIED: TEST-06 requirement in .planning/REQUIREMENTS.md]

### Pitfall 2: Upload Helper Silently Allows Resume Semantics

**What goes wrong:** Reusing `upload_audio()` would treat a 412 as success when `if_generation_match` is set. [VERIFIED: backend/pipeline/common/gcp_helper.py]

**Why it happens:** That helper was designed for idempotent ingestion retries, not immutable dataset-version publication. [VERIFIED: backend/pipeline/common/gcp_helper.py; VERIFIED: 03-CONTEXT.md]

**How to avoid:** Use sync `blob.upload_from_string(..., if_generation_match=0)` and convert `PreconditionFailed` into a hard dataset-version-exists failure. [VERIFIED: backend/pipeline/common/storage/gcs_uploader.py; CITED: https://docs.cloud.google.com/storage/docs/samples/storage-upload-file]

**Warning signs:** Tests assert 412 success or include `resume`, `force`, or `overwrite` flags. [VERIFIED: 03-CONTEXT.md]

### Pitfall 3: Gemini MIME Metadata Lies About Original Audio

**What goes wrong:** Existing `build_example()` always writes `audio/flac`, while Phase 3 may point at original MP3 audio. [VERIFIED: model/colabs/common/sft.py; VERIFIED: 03-CONTEXT.md]

**Why it happens:** Earlier code assumed segmented FLAC; Phase 3 deliberately runs before Phase 4 clip derivation. [VERIFIED: model/colabs/common/sft.py; VERIFIED: 03-CONTEXT.md]

**How to avoid:** Extend `build_example()` and `validate_example()` to preserve the same nested shape while accepting a caller-supplied verified MIME type; include tests for `audio/flac` and `audio/mpeg`. [VERIFIED: model/colabs/common/tests/test_sft.py; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]

**Warning signs:** Gemini JSONL for `.mp3` source URIs contains `"mimeType": "audio/flac"`. [VERIFIED: model/colabs/common/sft.py]

### Pitfall 4: Dataset Reports Become SFT Run Reports

**What goes wrong:** Reports start including tuned model IDs, training metrics, post-run eval, or comparisons. [VERIFIED: 03-CONTEXT.md]

**Why it happens:** Existing `pipeline.py` uses legacy `round_id` naming and mixes build/tune/eval workflow concepts. [VERIFIED: model/scripts/sft/pipeline.py]

**How to avoid:** Name Phase 3 report objects `dataset_version_report.*`, include artifact inventory and writer warnings only, and keep run metrics out. [VERIFIED: 03-CONTEXT.md]

**Warning signs:** Report schema fields mention endpoint, tuned model, experiment, WER after tuning, or run comparison. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/colabs/common/vertex.py]

### Pitfall 5: Canonical Rows Drift From Existing Eval Contracts

**What goes wrong:** Model writers rename `audio_filepath`, drop `offset`, or mutate existing eval manifests. [VERIFIED: model/colabs/common/manifest.py; VERIFIED: 03-CONTEXT.md]

**Why it happens:** Canonical dataset-version rows are richer than current eval `CanonicalRow`, but model/eval rows still need stable fields. [VERIFIED: model/scripts/sft/dataset_split/types.py; VERIFIED: model/colabs/common/manifest.py]

**How to avoid:** Keep enriched canonical manifests separate from model-input manifests, and never edit historical benchmark/eval files. [VERIFIED: 03-CONTEXT.md; VERIFIED: .planning/REQUIREMENTS.md]

**Warning signs:** Diffs touch `model/data/manifests/*.json`, notebooks, or existing eval fixture manifests without a direct test need. [VERIFIED: rg results over model/data/manifests; VERIFIED: MODL-08 in .planning/REQUIREMENTS.md]

## Code Examples

### Canonical Row Serialization

```python
def canonical_row(segment: LabeledSegment) -> dict[str, object]:
    return {
        "dataset_name": segment.dataset_name,
        "dataset_family": segment.dataset_family,
        "source_group": segment.source_group,
        "split": segment.split,
        "audio_uri": segment.audio_uri,
        "original_audio_uri": segment.original_audio_uri,
        "text": segment.text,
        "offset": segment.offset,
        "duration": segment.duration,
        "example_id": segment.example_id,
        "segment_id": segment.segment_id,
        "timestamp": segment.timestamp,
        "model_ready_audio_uri": segment.model_ready_audio_uri,
        "derived_audio_uri": segment.derived_audio_uri,
        "transformation_metadata": segment.transformation_metadata,
    }
```

Source: `LabeledSegment` fields and D-08 `raw_row` exclusion. [VERIFIED: model/scripts/sft/dataset_split/types.py; VERIFIED: 03-CONTEXT.md]

### NeMo Writer Row

```python
def nemo_row(segment: LabeledSegment) -> dict[str, object]:
    return {
        "audio_filepath": segment.audio_uri,
        "text": segment.text,
        "duration": segment.duration,
        "offset": segment.offset,
    }
```

Source: existing eval row contract and Phase 3 NeMo decision. [VERIFIED: model/colabs/common/manifest.py; VERIFIED: 03-CONTEXT.md]

### Gemini Writer Row With Verified MIME

```python
def gemini_row(
    segment: LabeledSegment,
    *,
    system_prompt: str,
    user_prompt: str,
    mime_type: str,
) -> dict[str, object]:
    example = build_example(
        audio_uri=segment.audio_uri,
        gt_text=segment.text,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        mime_type=mime_type,
    )
    if not validate_example(example):
        raise ModelWriterError(f"invalid Gemini example: row_index={segment.row_index}")
    return example
```

Source: recommended small extension to existing `common.sft` helper based on current audio-tuning docs. [VERIFIED: model/colabs/common/sft.py; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]

## State of the Art

| Old Approach | Current Approach | When Changed / Verified | Impact |
|--------------|------------------|--------------------------|--------|
| Vertex AI docs as the only Gemini tuning source | Agent Platform docs are the current source for Gemini model support | Verified 2026-05-27; Vertex page says services are now part of Gemini Enterprise Agent Platform. [CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning; CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning] | Use Agent Platform model list for Gemini 3.1 Flash-Lite support. [CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning] |
| Upload without preconditions | `if_generation_match=0` for create-only object writes | Verified 2026-05-27 via GCS docs. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration] | Prevents per-object races after prefix preflight. [CITED: https://docs.cloud.google.com/storage/docs/samples/storage-upload-file] |
| Gemini helper assumes only FLAC | Audio tuning docs show `audio/mpeg` example; helper should accept verified MIME values | Verified 2026-05-27. [VERIFIED: model/colabs/common/sft.py; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune] | Planner should include a targeted helper/test update. [VERIFIED: model/colabs/common/tests/test_sft.py] |

**Deprecated/outdated:**

- Treating the older Vertex model list as complete is outdated because the Vertex docs now redirect currency to Agent Platform docs. [CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini-supervised-tuning]
- Legacy flat Gemini SFT examples such as `{prompt, response}` are invalid for this repo because existing tests reject them and current docs use nested `contents`. [VERIFIED: model/colabs/common/tests/test_sft.py; CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning-prepare]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | If Phase 3 encounters original audio formats beyond FLAC and MP3, MIME support should be treated as unverified until checked against current Gemini audio docs or a live preflight. [ASSUMED] | Common Pitfalls / Open Questions | Gemini JSONL could contain an unsupported MIME type or unnecessarily block a usable source format. |

## Open Questions (RESOLVED)

1. **How broad should Gemini MIME support be in Phase 3?** [VERIFIED: model/colabs/common/sft.py; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]
   - What we know: Existing code supports FLAC only, while official audio tuning docs show MP3 via `audio/mpeg`. [VERIFIED: model/colabs/common/sft.py; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]
   - What's unclear: The docs page read in this session does not enumerate every accepted audio MIME type for tuning examples. [CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]
   - Resolution: Phase 3 supports the verified `audio/flac` and `audio/mpeg` MIME types. Unknown audio extensions or MIME values hard-fail per D-20 instead of emitting false metadata; future formats require rechecking current Google tuning docs or adding a live preflight-backed test. [VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| `uv` | Model test/runtime execution | Yes [VERIFIED: uv --version] | Shell has 0.11.2; project pins 0.9.28. [VERIFIED: uv --version; VERIFIED: .tool-versions] | Use pinned `mise install` if version drift matters. [VERIFIED: .mise.toml] |
| Python | Model tooling | Yes [VERIFIED: python3 --version; VERIFIED: uv run --project model] | Shell `python3` is 3.12.13; `uv run --project model` used CPython 3.13.12; project pins 3.13.2. [VERIFIED: python3 --version; VERIFIED: uv run --project model; VERIFIED: .tool-versions] | Use `mise`/`uv` project environment instead of raw `python3`. [VERIFIED: .mise.toml] |
| `gcloud` | Real GCS/Vertex authentication outside tests | Yes [VERIFIED: gcloud --version] | Google Cloud SDK 565.0.0. [VERIFIED: gcloud --version] | Unit tests should use fake clients; live generation requires ADC. [VERIFIED: .planning/codebase/TESTING.md] |
| Context7 CLI via `npx` | Current cloud/API docs | Yes [VERIFIED: npx --version; VERIFIED: ctx7 CLI output] | `npx` 10.9.7. [VERIFIED: npx --version] | Web official docs were used only after Context7 for specific Agent Platform pages. [VERIFIED: ctx7 CLI output; CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning] |
| Live GCS bucket access | Manual generation command | Not verified in this research to avoid probing credentials. [ASSUMED] | Unknown. [ASSUMED] | Unit tests must not require live GCS. [VERIFIED: .planning/codebase/TESTING.md] |

**Missing dependencies with no fallback:** None for planning and unit-test implementation. [VERIFIED: Environment Availability table]

**Missing dependencies with fallback:** Live GCS credentials were not verified; fake clients cover automated Phase 3 tests. [ASSUMED; VERIFIED: .planning/codebase/TESTING.md]

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | `pytest` 9.0.3 for model/SFT tests. [VERIFIED: uv run --project model --extra dev] |
| Config file | `model/pyproject.toml` for `common` tests; `model/scripts/sft/tests` use direct path/PYTHONPATH patterns. [VERIFIED: model/pyproject.toml; VERIFIED: model/scripts/sft/tests/test_pipeline_build.py] |
| Quick run command | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex pytest model/scripts/sft/tests/test_dataset_artifacts.py model/scripts/sft/tests/test_model_writers.py -q` [VERIFIED: model/scripts/sft/tests; VERIFIED: model/pyproject.toml] |
| Full suite command | `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex --extra optimizer pytest model/scripts/sft/tests model/colabs/common/tests -q` [VERIFIED: .planning/codebase/TESTING.md; VERIFIED: model/pyproject.toml] |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| ARTF-01 | Layout resolves locked dataset-version root and artifact subpaths. [VERIFIED: 03-CONTEXT.md] | unit | `pytest model/scripts/sft/tests/test_dataset_artifacts.py::test_layout_uses_dataset_version_root -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| ARTF-02 / TEST-06 | Existing prefix and object precondition failures abort generation. [VERIFIED: .planning/REQUIREMENTS.md] | unit | `pytest model/scripts/sft/tests/test_dataset_artifacts.py::test_existing_prefix_fails -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| ARTF-03 | Canonical train/eval manifests contain enriched rows and omit `raw_row`. [VERIFIED: 03-CONTEXT.md] | unit | `pytest model/scripts/sft/tests/test_dataset_canonical.py -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| ARTF-04 | Per-dataset train/eval slices are grouped by dataset and split. [VERIFIED: .planning/REQUIREMENTS.md] | unit | `pytest model/scripts/sft/tests/test_dataset_canonical.py::test_per_dataset_slices -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| ARTF-05 | JSON and Markdown reports include required generation fields. [VERIFIED: 03-CONTEXT.md] | unit | `pytest model/scripts/sft/tests/test_dataset_reports.py -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| ARTF-06 / MODL-08 | No existing eval manifests are modified by generation. [VERIFIED: .planning/REQUIREMENTS.md] | regression/unit | `pytest model/scripts/sft/tests/test_dataset_artifacts.py::test_generation_targets_only_new_artifacts -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| MODL-01 / MODL-02 | NeMo rows and config fragment have expected fields. [VERIFIED: .planning/REQUIREMENTS.md] | unit | `pytest model/scripts/sft/tests/test_model_writers.py::test_nemo_writer_shape -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| MODL-03 / MODL-04 | Whisper rows preserve metadata and report >30s warnings. [VERIFIED: 03-CONTEXT.md] | unit | `pytest model/scripts/sft/tests/test_model_writers.py::test_whisper_writer_shape_and_warnings -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| MODL-05 / MODL-06 / MODL-07 | Gemini JSONL and config use current nested SFT shape and configurable base model. [VERIFIED: .planning/REQUIREMENTS.md] | unit | `pytest model/scripts/sft/tests/test_model_writers.py::test_gemini_writer_shape_and_config -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |
| TEST-05 | All model writer output shapes covered together. [VERIFIED: .planning/REQUIREMENTS.md] | unit | `pytest model/scripts/sft/tests/test_model_writers.py -q` | No, Wave 0. [VERIFIED: find model/scripts/sft/tests] |

### Sampling Rate

- **Per task commit:** Run the specific new test file plus existing touched common tests. [VERIFIED: .planning/codebase/TESTING.md]
- **Per wave merge:** Run `PYTHONPATH=model/scripts/sft:model/colabs uv run --project model --extra dev --extra scoring --extra vertex --extra optimizer pytest model/scripts/sft/tests model/colabs/common/tests -q`. [VERIFIED: model/pyproject.toml; VERIFIED: .planning/codebase/TESTING.md]
- **Phase gate:** Full model/SFT suite green before `$gsd-verify-work`. [VERIFIED: .planning/config.json]

### Wave 0 Gaps

- [ ] `model/scripts/sft/tests/test_dataset_artifacts.py` covers ARTF-01, ARTF-02, ARTF-06, TEST-06. [VERIFIED: .planning/REQUIREMENTS.md]
- [ ] `model/scripts/sft/tests/test_dataset_canonical.py` covers ARTF-03 and ARTF-04. [VERIFIED: .planning/REQUIREMENTS.md]
- [ ] `model/scripts/sft/tests/test_model_writers.py` covers MODL-01 through MODL-07 and TEST-05. [VERIFIED: .planning/REQUIREMENTS.md]
- [ ] `model/scripts/sft/tests/test_dataset_reports.py` covers ARTF-05 and D-18/D-19 report boundaries. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: 03-CONTEXT.md]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | No for Phase 3 unit implementation; live GCS uses ADC/IAM outside code. [VERIFIED: .planning/ROADMAP.md; ASSUMED] | Do not embed credentials; rely on Google auth clients. [VERIFIED: user-provided AGENTS.md instructions; VERIFIED: model/scripts/sft/pipeline.py] |
| V3 Session Management | No browser or session surface in Phase 3. [VERIFIED: .planning/ROADMAP.md] | Not applicable. [VERIFIED: .planning/ROADMAP.md] |
| V4 Access Control | Yes at GCS/IAM boundary. [ASSUMED] | Use the authenticated storage client and do not bypass bucket IAM with signed/public URLs. [VERIFIED: model/colabs/common/gcs_utils.py; ASSUMED] |
| V5 Input Validation | Yes. [VERIFIED: .planning/REQUIREMENTS.md] | Validate `gs://` URIs, required fields, split values, writer schemas, and config ratios. [VERIFIED: model/scripts/sft/dataset_split/config.py; VERIFIED: model/scripts/sft/dataset_split/leakage.py; VERIFIED: model/colabs/common/sft.py] |
| V6 Cryptography | No custom cryptography in Phase 3. [VERIFIED: .planning/ROADMAP.md] | Do not hand-roll signing, hashing, encryption, or credential handling. [ASSUMED] |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Artifact overwrite or confused dataset-version identity | Tampering | Prefix preflight plus per-object `if_generation_match=0`. [VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration] |
| Raw input row leakage into generated artifacts | Information Disclosure | Omit `raw_row` from canonical manifests and reports. [VERIFIED: 03-CONTEXT.md] |
| Malformed manifest rows corrupting metrics or tuning data | Tampering | Fail loud on structural errors and use existing validators. [VERIFIED: model/colabs/common/manifest.py; VERIFIED: model/scripts/sft/preflight.py] |
| Credential exposure through docs/logs/reports | Information Disclosure | Do not include credentials in queries, logs, or generated reports. [VERIFIED: user-provided AGENTS.md instructions; ASSUMED] |

## Sources

### Primary (HIGH confidence)

- Context7 `/googleapis/python-storage` - checked upload overwrite/default behavior, `if_generation_match`, and upload APIs. [VERIFIED: ctx7 CLI output]
- Context7 `/websites/cloud_google_vertex-ai` - checked supervised tuning fields and hyperparameter names. [VERIFIED: ctx7 CLI output]
- Google Cloud Storage Python Client `Client.list_blobs` docs - checked prefix filtering and `max_results`. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.client.Client]
- Google Cloud Storage generation precondition docs - checked `if_generation_match=0` create-only semantics. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration]
- Google Cloud upload sample - checked create-only upload sample. [CITED: https://docs.cloud.google.com/storage/docs/samples/storage-upload-file]
- Gemini Enterprise Agent Platform supervised tuning docs - checked Gemini 3.1 Flash-Lite support and dataset limits. [CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning]
- Gemini Enterprise Agent Platform tuning job docs - checked `trainingDatasetUri`, `validationDatasetUri`, and hyperparameter fields. [CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-use-supervised-tuning]
- Gemini supervised tuning data-prep docs - checked nested `systemInstruction`/`contents`/`fileData` shape. [CITED: https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/gemini-supervised-tuning-prepare]
- Gemini audio tuning docs - checked audio JSONL example using `audio/mpeg`. [CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]
- Phase 3 CONTEXT and project requirements. [VERIFIED: .planning/phases/03-gcs-artifacts-and-model-writers/03-CONTEXT.md; VERIFIED: .planning/REQUIREMENTS.md]

### Secondary (MEDIUM confidence)

- Existing codebase tests and helpers for GCS, SFT, Vertex, HF, and NeMo contracts. [VERIFIED: model/colabs/common/tests; VERIFIED: backend/pipeline/common/storage/tests/test_gcs_uploader.py]
- Existing GSD codebase maps for stack, testing, and conventions. [VERIFIED: .planning/codebase/STACK.md; VERIFIED: .planning/codebase/TESTING.md; VERIFIED: .planning/codebase/CONVENTIONS.md]

### Tertiary (LOW confidence)

- MIME support beyond verified FLAC and MP3 was not confirmed in this session. [ASSUMED]

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH for local Python/GCS/test dependencies because versions and docs were verified in this session. [VERIFIED: uv run --project model; VERIFIED: Context7 /googleapis/python-storage]
- Architecture: HIGH because it follows locked Phase 3 decisions and existing Phase 1/2 module boundaries. [VERIFIED: 03-CONTEXT.md; VERIFIED: model/scripts/sft/dataset_split]
- Pitfalls: HIGH for GCS overwrite, prefix protection, report scope, and raw-row exclusion; MEDIUM for non-FLAC Gemini MIME behavior beyond MP3. [CITED: https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration; VERIFIED: 03-CONTEXT.md; CITED: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/tune_gemini/audio_tune]

**Research date:** 2026-05-27 [VERIFIED: system date]
**Valid until:** 2026-06-03 for Gemini/Agent Platform behavior and 2026-06-26 for repo-local architecture. [ASSUMED]
