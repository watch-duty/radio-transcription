# Research: Pitfalls

## Critical Mistakes To Prevent

### Row-Level Random Split

Warning sign: the split function samples rows or audio files directly.

Prevention: the split unit must be Source Group. Compute rows/examples only after source assignment.

### Echo Source Ambiguity

Warning sign: `echo_name` is used without `area_code`, or rows missing both are accepted.

Prevention: use the documented cascade and fail unresolved cases. The validated CSV has duplicate Echo names across areas, so guessing would leak.

### Fire Notification Day UUID Leakage

Warning sign: `group_id` from collection day is used as the split key.

Prevention: use stream path/location. The day UUID groups one sampling pass, not the upstream source.

### Reusing Historical Eval Semantics Accidentally

Warning sign: generated SFT Eval Split is described as a hidden holdout or overwrites existing benchmark manifests.

Prevention: keep existing eval manifests unchanged. Name this explicitly as SFT Eval Split.

### Silent Dropping Of Bad Rows

Warning sign: source parse failures, empty text rows, or unsupported audio rows disappear without a report.

Prevention: all exclusions must be counted and written to reports. Hard failures should stop generation when ambiguity can leak.

### Model Input Drift

Warning sign: Gemini prompt shape or media MIME type is duplicated in a new script while existing `common.sft` and prompt drift guards exist.

Prevention: route Gemini example generation through shared helpers or update shared helpers with tests.

### Audio Transformation Drift

Warning sign: every model writer independently slices/resamples audio.

Prevention: centralize audio planning and provenance. Model writers should consume planned canonical example URIs.

### GCS Overwrite Risk

Warning sign: rerunning a dataset version silently overwrites manifests or derived clips.

Prevention: fail if `gs://wd-transcription-data/sft/{dataset_version_id}/` exists unless `--force` is explicit and recorded.

### Over-Balancing At The Cost Of Leakage

Warning sign: optimizer allows a source group to be split to improve duration/month balance.

Prevention: leakage gates are hard constraints; balance is best-effort and reported.

### Gemini Model List Drift

Warning sign: code relies on the older Vertex AI supervised-tuning supported-model page and rejects Gemini 3.1 Flash-Lite.

Prevention: use the current Gemini Enterprise Agent Platform supervised-tuning docs as the source of truth. They list Gemini 3.1 Flash-Lite as supported, while the base model should still remain configurable and validated during tuning-job creation.

## Phase Placement

- Source-key ambiguity and leakage gates: Phase 1.
- Balance scoring and deterministic split search: Phase 2.
- GCS artifact layout and model writers: Phase 3.
- Audio derivation/provenance execution: Phase 4.
- Documentation and full CLI verification: final integration phase.
