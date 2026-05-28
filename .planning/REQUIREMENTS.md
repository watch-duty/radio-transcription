# Requirements: SFT Dataset Versioning

**Defined:** 2026-05-27
**Core Value:** Every SFT run must train and compare models on the same auditable dataset version without source leakage between train and SFT Eval Split.

## v1 Requirements

### Input Registry

- [x] **INPT-01**: User can define one dataset-version job with `dataset_version_id`, train/eval ratio, output GCS prefix, and one or more input datasets.
- [x] **INPT-02**: User can configure each input dataset with dataset name, dataset family, manifest `gs://` URI, source-key strategy, and optional sidecar source map `gs://` URI.
- [x] **INPT-03**: The splitter can read configured JSON/JSONL manifests and sidecar source maps from `gs://` URIs using existing repository GCS helpers where practical; tests may use fake readers to cover success and failure classes without real GCS access.
- [x] **INPT-04**: Rows with empty or missing normalized text are excluded from SFT examples and counted in the CLI/log validation summary.

### Source Identity

- [x] **SRC-01**: Broadcastify Calls rows resolve Source Group as `bcfy_calls:<groupId>`.
- [x] **SRC-02**: Broadcastify Feeds rows resolve Source Group as `bcfy_feeds:<feedId>`.
- [x] **SRC-03**: Echo rows resolve Source Group as `echo:<area_code>/<echo_name>` using explicit fields, URL/S3 key parsing, sidecar source map, or unique CSV match.
- [x] **SRC-04**: Echo rows fail source-key validation when `area_code` is missing and `echo_name` is ambiguous across area codes.
- [x] **SRC-05**: Fire Notification rows resolve Source Group from stream path/location, not collection day UUID.
- [x] **SRC-06**: Every source-key extractor has focused tests for valid, missing, and ambiguous inputs.

### Split And Leakage

- [x] **SPLT-01**: User can generate an 80:20 train/SFT Eval Split by assigning whole Source Groups to exactly one split.
- [x] **SPLT-02**: Split generation emits the chosen Source Group assignment, score report, config, and algorithm metadata needed to audit the selected split.
- [x] **SPLT-03**: Split validation fails if any Source Group appears in both train and SFT Eval Split.
- [x] **SPLT-04**: Split validation fails if any original source audio appears in both train and SFT Eval Split.
- [x] **SPLT-05**: Split validation fails if any model-ready audio URI appears in both train and SFT Eval Split.
- [x] **SPLT-06**: Split validation fails when a configured dataset produces zero valid SFT examples.
- [x] **SPLT-07**: Split optimization considers dataset family, source count, row count, duration, duration buckets, and transcript-length buckets, while month/date/hour distributions are report-only when available.
- [x] **SPLT-08**: Split reports show requested ratio, actual row ratio, duration ratio, source ratio, and balance deltas by correlated factor.

### GCS Artifacts

- [x] **ARTF-01**: User can write a dataset version under `gs://wd-transcription-data/sft/{dataset_version_id}/`.
- [x] **ARTF-02**: Generation fails if the dataset version path already exists unless an explicit force flag is provided and recorded.
- [x] **ARTF-03**: The generator writes canonical train/eval JSONL manifests.
- [x] **ARTF-04**: The generator writes per-dataset train/eval JSONL slices.
- [x] **ARTF-05**: The generator writes machine-readable JSON reports and a human-readable Markdown summary.
- [x] **ARTF-06**: Generated manifests and derived audio are not committed to Git.

### Audio And Provenance

- [ ] **AUD-01**: The planner reuses an existing standalone supported clip when a row already points to one utterance clip.
- [ ] **AUD-02**: The planner derives a clip only when a labeled row points into a longer source audio file by offset/duration.
- [ ] **AUD-03**: Derived clips preserve the least-transforming reliable audio format accepted by target writers, with WAV fallback when exact source-format slicing is unreliable.
- [ ] **AUD-04**: Multichannel input is mixed to mono when deriving clips.
- [ ] **AUD-05**: The generator does not add padding and does not resample by default unless a target-specific writer requires it.
- [ ] **AUD-06**: Every SFT example records provenance for original audio URI, offset, duration, source group, split, reuse/derived decision, and transformation metadata.

### Model Inputs

- [x] **MODL-01**: NeMo writer emits train/eval manifests with `audio_filepath`, `text`, and `duration`.
- [x] **MODL-02**: NeMo writer emits a config fragment that references train and validation manifest paths.
- [x] **MODL-03**: Whisper writer emits train/eval loader-friendly manifests with audio URI/path, text, duration, source metadata, and preprocessing recommendations.
- [x] **MODL-04**: Whisper writer records or enforces the sub-30-second example constraint needed to avoid Whisper feature-extractor truncation.
- [x] **MODL-05**: Gemini writer emits Vertex SFT JSONL with `systemInstruction`, `contents`, `fileData`, `mimeType`, and target transcript text.
- [x] **MODL-06**: Gemini writer emits a tuning config with train dataset URI, validation dataset URI, base model name, region, adapter size, epoch count, and learning-rate multiplier.
- [x] **MODL-07**: Gemini writer supports Gemini 3.1 Flash-Lite as a documented supervised-tuning target while keeping the base model configurable.
- [x] **MODL-08**: Existing benchmark/eval manifests remain unchanged.

### CLI And Reports

- [ ] **CLI-01**: User can run a dry run that validates inputs, computes split assignment, and writes local reports without uploading derived audio.
- [ ] **CLI-02**: User can run a generation command that writes canonical/model inputs and reports to GCS.
- [ ] **CLI-03**: The CLI exits nonzero on hard validation failures and prints the report path or failure report location.
- [ ] **CLI-04**: The report bundle includes leakage, balance, source-key failures, excluded rows, and transformations.
- [ ] **CLI-05**: Documentation explains the terms Source Group, Labeled Segment, SFT Example, SFT Eval Split, and Dataset Version.

### Tests

- [x] **TEST-01**: Tests cover all dataset-specific source-key extractors.
- [x] **TEST-02**: Tests cover Source Group split assignment, per-dataset train/eval coverage, and the seed-free config contract.
- [x] **TEST-03**: Tests cover leakage-gate failures for source overlap, original-audio overlap, and model-ready URI overlap.
- [x] **TEST-04**: Tests cover balance scoring and report contents.
- [x] **TEST-05**: Tests cover NeMo, Whisper, and Gemini model-writer output shapes.
- [x] **TEST-06**: Tests cover existing-path protection for dataset versions.

## v2 Requirements

### Training Execution

- **RUN-01**: Submit NeMo custom-training jobs from generated dataset artifacts.
- **RUN-02**: Submit Whisper custom-training jobs from generated dataset artifacts.
- **RUN-03**: Submit Gemini SFT jobs and post-run evaluation from generated dataset artifacts.
- **RUN-04**: Track model-run metadata and compare model outputs across dataset versions.

### Scaling

- **SCAL-01**: Emit tarred/WebDataset NeMo artifacts for large training runs.
- **SCAL-02**: Support sharded canonical/model manifests for very large datasets.
- **SCAL-03**: Add a dashboard or report index for comparing dataset versions.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Sampling new upstream API audio | This project consumes existing labeled manifests; sampling belongs in dataset acquisition workflows. |
| Replacing historical benchmark/eval manifests | SFT dataset versions are separate artifacts. |
| Hidden holdout creation | The requested eval side is an SFT Eval Split that may be used for validation and model selection. |
| Perfect balancing across every correlated factor | Source-group leakage prevention is the hard constraint; balance is optimized and reported. |
| Committing generated manifests/audio to Git | Generated proprietary artifacts belong in GCS. |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| INPT-01 | Phase 1 | Complete |
| INPT-02 | Phase 1 | Complete |
| INPT-03 | Phase 1 | Complete |
| INPT-04 | Phase 1 | Complete |
| SRC-01 | Phase 1 | Complete |
| SRC-02 | Phase 1 | Complete |
| SRC-03 | Phase 1 | Complete |
| SRC-04 | Phase 1 | Complete |
| SRC-05 | Phase 1 | Complete |
| SRC-06 | Phase 1 | Complete |
| TEST-01 | Phase 1 | Complete |
| SPLT-01 | Phase 2 | Complete |
| SPLT-02 | Phase 2 | Complete |
| SPLT-03 | Phase 2 | Complete |
| SPLT-04 | Phase 2 | Complete |
| SPLT-05 | Phase 2 | Complete |
| SPLT-06 | Phase 2 | Complete |
| SPLT-07 | Phase 2 | Complete |
| SPLT-08 | Phase 2 | Complete |
| TEST-02 | Phase 2 | Complete |
| TEST-03 | Phase 2 | Complete |
| TEST-04 | Phase 2 | Complete |
| ARTF-01 | Phase 3 | Complete |
| ARTF-02 | Phase 3 | Complete |
| ARTF-03 | Phase 3 | Complete |
| ARTF-04 | Phase 3 | Complete |
| ARTF-05 | Phase 3 | Complete |
| ARTF-06 | Phase 3 | Complete |
| MODL-01 | Phase 3 | Complete |
| MODL-02 | Phase 3 | Complete |
| MODL-03 | Phase 3 | Complete |
| MODL-04 | Phase 3 | Complete |
| MODL-05 | Phase 3 | Complete |
| MODL-06 | Phase 3 | Complete |
| MODL-07 | Phase 3 | Complete |
| MODL-08 | Phase 3 | Complete |
| TEST-05 | Phase 3 | Complete |
| TEST-06 | Phase 3 | Complete |
| AUD-01 | Phase 4 | Pending |
| AUD-02 | Phase 4 | Pending |
| AUD-03 | Phase 4 | Pending |
| AUD-04 | Phase 4 | Pending |
| AUD-05 | Phase 4 | Pending |
| AUD-06 | Phase 4 | Pending |
| CLI-01 | Phase 5 | Pending |
| CLI-02 | Phase 5 | Pending |
| CLI-03 | Phase 5 | Pending |
| CLI-04 | Phase 5 | Pending |
| CLI-05 | Phase 5 | Pending |

**Coverage:**
- v1 requirements: 49 total
- Mapped to phases: 49
- Unmapped: 0

---
*Requirements defined: 2026-05-27*
*Last updated: 2026-05-28 after Phase 3 completion*
