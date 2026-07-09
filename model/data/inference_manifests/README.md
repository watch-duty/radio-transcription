This directory saves inference outputs for various models and dataset
combinations, so we can compute metrics over the saved outputs. The table below
documents issue trackers for each json in this directory, so we can trace what
each inference output implies.

## Artifact Types

A source/canonical manifest is the row-per-segment input dataset. It contains
the canonical fields documented in `../manifests/README.md`, including
`audio_filepath`, `text`, `offset`, `duration`, `example_id`, `segment_id`,
and optional `split`, `dataset`, and `source_audio` metadata.

A normalized inference manifest is the scorer-ready output for one model family
and one run. It preserves the source/canonical manifest rows, requires reference
`text` on every row, and adds at most one prediction field named
`pred_text_<model_family_slug>` per row. The field is present only when a
prediction record exists for that row; an empty string value means the
prediction record existed and contained empty text.

A merged comparison manifest is a derived wide artifact that contains multiple
`pred_text_*` fields on each row. These are useful for side-by-side analysis,
but they are not the default output of new SFT eval runs.

A raw provider output is the service-specific response format returned by a
model provider, such as Vertex batch prediction JSONL. Raw provider outputs are
kept for provenance and debugging, but downstream scoring should consume a
normalized inference manifest or a merged comparison manifest.

SFT run state is the durable control-plane record under `sft/runs/<round-id>/`.
It contains `config.json`, tuning status, canonical manifests, Gemini model
inputs, and raw eval batch output. It is separate from normalized inference
manifests, which live under `inference_manifests/`.

## Standard GCS Layout

New single-series artifacts should use:

```text
inference_manifests/<inference_dataset_slug>/<model_family_slug>/<run_id>/<artifact_label>.jsonl
```

`inference_dataset_slug` identifies the evaluated corpus/split, such as
`echo/eval`. `model_family_slug` identifies the model family, such as
`gemini_3_1_flash_lite`. `run_id` identifies the experiment or tuning round.
`artifact_label` distinguishes outputs within a run, such as `base`, `tuned`,
or `checkpoint_1`.

Scorer consumers discover `pred_text_*` fields and should treat an absent
prediction field as an empty hypothesis, so they can score a normalized
inference manifest with at most one prediction field per row or a merged
comparison manifest with multiple prediction fields.

Wide base-vs-tuned or model-vs-model comparison manifests are derived artifacts
and are not produced by default by the Gemini SFT Phase 7 workflow.

| JSON manifest | Linear issue tracker link | Comment |
| ------------- | ------------------------- | ------- |
| playground\_parakeet\_and\_canary\_flash.json | https://linear.app/watchduty/issue/GOO-20/run-inference-with-3-chosen-models#comment-6705c94d | |
| playground\_parakeet\_and\_canary\_flash\_and\_chirp\_with\_context.jsonl | https://linear.app/watchduty/issue/GOO-23/setup-chirp-v3-evaluation-to-produce-nemo-consistent-json-output | Produced using full audio file |
| playground\_parakeet\_and\_canary\_flash\_and\_chirp.jsonl | https://linear.app/watchduty/issue/GOO-23/setup-chirp-v3-evaluation-to-produce-nemo-consistent-json-output | Produced using the specified segments in the manifest |
| playground\_parakeet\_and\_canary\_flash\_and\_gemma3n\_e2b\_it.json | https://linear.app/watchduty/issue/GOO-34/run-gemma-3n-inference-within-nemo | |
