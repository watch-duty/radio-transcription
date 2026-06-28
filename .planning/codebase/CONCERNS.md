# Concerns

## Current Worktree State

At map time the repository had been merged with `origin/main`, but the worktree
still contained unrelated experiment changes:

- Modified `model/scripts/sft/score_gemini_sft_checkpoints_online.py`.
- Multiple untracked model inference manifests, local SFT TOML files, scoring
  summaries, `model/tests/gemini_sft/test_checkpoint_scorer.py`, and `results/`.

This map intentionally did not inspect or commit those local artifacts as
source of truth.

## Operational Complexity

The repo spans live ingestion, cloud functions, APIs, frontend, ASR research,
Vertex SFT, Terraform, and local emulation. New contributors can easily choose
the wrong runtime unless docs keep pointing to:

- `CONTRIBUTING.md` for app/backend/frontend local development.
- `ASR_CONTRIBUTING.md` for model and ASR work.
- `model/scripts/sft/README.md` for Gemini SFT.
- `.mise.toml` for actual local commands.
- `CONTEXT.md` for domain terms.

## Generated Artifacts In Source Tree

Several workflows generate files inside the repo:

- Protobuf Python files under `backend/pipeline/schema_types`.
- Frontend API generated TSOA routes/OpenAPI files.
- Local `results/` mirrors for SFT/eval runs.
- Local model inference manifests and local SFT configs.

Generated source outputs needed by runtime/CI should remain explicit. Local run
artifacts should stay out of commits unless a task asks for artifact publishing.

## Gemini SFT Eval Gap

`gemini-sft eval` supports base and tuned endpoint batch inference when Vertex
batch accepts the model. Checkpoint evaluation currently needs the separate
`model/scripts/sft/score_gemini_sft_checkpoints_online.py` script because
Vertex batch inference does not accept tuned checkpoint endpoint resources.

This creates two reporting paths:

- Config-driven batch eval under `gemini_sft.evaluate`.
- Online checkpoint scoring script.

Keep metric names, prompt/request construction, prior-context handling, safety
settings, and report columns aligned across both paths.

## Empty Response Terminology

There are two related but different concepts:

- Exact empty response rate: model returned no text.
- Historical empty/unintelligible rate: model returned no text or exactly
  `[UNINTELLIGIBLE]`.

Reports should not label both as "empty" without explanation. Production
transcription also converts empty engine output to `[UNINTELLIGIBLE]`, which is
not the same as preserving raw eval empties.

## Prior Context Semantics

Gemini prior context is not static data in the manifest. It is derived per row
by grouping same-source rows, sorting by source offset/order, and taking up to
`prior_context_count` usable previous transcripts. Rows with empty text or
`[UNINTELLIGIBLE]` are excluded from future histories.

Any future config or CLI simplification must preserve this dynamic construction
or make the difference explicit.

## Ingestion Registry Drift

The ingestion source registry is intentionally checked at startup, but adding a
source still touches several files and tests. A contributor can miss one of:

- `SourceType`
- seed/source type data
- `source_runtime_specs.py`
- `router.py`
- collector tests and integration tests

Maintain the startup invariant and keep source-addition docs/tests close to the
registry.

## Cloud-Cost Boundaries

Model tuning and Vertex eval can spend money. The SFT CLI has preflight and cost
confirmation, but one-off scripts and notebooks may bypass some safety rails.

Recommended guardrails:

- Keep paid Vertex calls behind explicit CLI commands or scripts with clear
  confirmations.
- Keep unit tests mocked at GCS/Vertex boundaries.
- Prefer smoke limits before full eval/scoring.
- Log GCS output URIs and model/resource names for reproducibility.

## Local E2E Resource Use

Full Docker Compose E2E starts many services and may be CPU/memory heavy. Local
Whisper adds model-download and inference cost to the host. Prefer targeted
component/API tests when debugging storage or API behavior, and use `safe-run`
for heavy non-Docker commands.

## Documentation Drift

The repo has multiple docs with overlapping setup instructions. When changing
SFT, ASR runtime, local dev commands, source routing, or test tasks, update the
nearest owner doc and then check for references in:

- `README.md`
- `CONTRIBUTING.md`
- `ASR_CONTRIBUTING.md`
- `CONTEXT.md`
- `model/scripts/sft/README.md`
- `.mise.toml`
