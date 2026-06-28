# Conventions

## Source Of Truth

- Use `CONTEXT.md` for domain terminology.
- Use `.mise.toml` for local task names.
- Use root `pyproject.toml` for backend Python package, lint, type, and pytest
  configuration.
- Use `model/pyproject.toml` for model package dependencies and `gemini-sft`
  console entry point.
- Use `model/scripts/sft/README.md` for Gemini SFT workflow semantics.
- Use GCS `config.json` under `gs://<bucket>/sft/runs/<round-id>/` as the
  durable SFT run state.

## Python

Project guidance is in `.github/instructions/PYTHON_STYLE.instructions.md`.

Important local conventions:

- Prefer full package imports over relative imports.
- Use built-in exception classes and avoid catch-all exception handling except
  at isolation boundaries.
- Avoid mutable global state unless it is deliberately internal.
- Use 4-space indentation and 80-character line length.
- Use Google-style docstrings where docstrings are needed.
- Group imports as future, standard library, third-party, local.
- Keep public APIs typed and avoid `assert` for application logic.

Ruff is configured in root `pyproject.toml` with `select = ["ALL"]` and a large
explicit ignore list. Do not add unsorted ignore entries; `.mise.toml` contains
a guard for sorted Ruff lists.

## TypeScript

Project guidance is in `.github/instructions/JS_TS_STYLE.instructions.md`.

Important local conventions:

- Prefer named exports; avoid default exports.
- Use `const` by default and `let` only when reassignment is needed.
- Do not use `var`.
- Prefer relative imports within the same logical project.
- Keep frontend files formatted by Prettier and linted by ESLint.
- API route/spec generation is part of the frontend API build contract.

## Configuration

- Root `.mise.toml` is the contributor task index.
- `.tool-versions` pins local tool versions for Mise.
- Root `.env` is loaded by Mise when present.
- Docker Compose uses `local_dev/LOCAL.env` for local service wiring.
- Frontend remote development uses generated `.env.local` style files under
  `frontend/api` and `frontend/transcription-ui`.
- Do not commit real Gemini SFT run configs; commit examples/placeholders only.

## Manifests And ASR Artifacts

Canonical manifests are row-per-audio-segment JSONL. The important required
fields are documented in `CONTEXT.md`:

- `audio_filepath`
- `text`
- `offset`
- `duration`
- `example_id`
- `segment_id`

The model pipeline preserves canonical manifests until a provider-specific
conversion boundary. Gemini SFT JSONL is derived from canonical train and
validation manifests during `gemini-sft prepare`. Eval manifests stay canonical
until `gemini-sft eval` builds batch-inference requests.

## Gemini Prompts And Requests

Canonical Gemini prompt/request state lives in importable code:

- `model/src/common/gemini/prompts.py`
- `model/src/common/gemini/context.py`
- `model/src/common/gemini/vertex.py`
- `model/src/common/gemini/tuning_data.py`

Notebook and CLI drift should be avoided by importing these helpers. The test
suite includes drift guards for shared prompt/request behavior.

SFT prompt overrides must be inline TOML strings. Prompt files and `@file`
references are rejected so resolved prompt text is copied to GCS `config.json`
and resume/eval does not depend on one developer's workstation.

## SFT Run State

Gemini SFT run state is GCS-authoritative:

```text
gs://<bucket>/sft/runs/<round-id>/
  run_config.toml
  config.json
  status.json
  manifests/canonical/
  model_inputs/gemini/
  preflight/report.json
  tuning/status.json
  evals/
```

Local `results/<round-id>/` files are a cache/mirror. A workflow should be able
to resume from GCS state even after local process exit.

## Safety And Empty Output Semantics

Gemini batch/online request construction currently sets:

- `temperature = 0.0`
- `max_output_tokens = 512`
- safety thresholds `BLOCK_NONE` for hate speech, sexually explicit,
  dangerous content, and harassment categories

Production transcription converts empty engine output to `[UNINTELLIGIBLE]`.
Model scoring tracks both exact empty responses and a historical
empty/unintelligible rate. Keep metric names explicit when reporting results.

## Git Hygiene

- Do not commit local `results/`, local SFT configs, or downloaded/generated
  inference manifests unless a task explicitly asks for an artifact commit.
- Keep commits scoped. This codebase map should only stage `.planning/codebase`.
- Do not overwrite or revert unrelated dirty worktree files.
