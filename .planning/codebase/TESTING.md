# Testing

## Test Surfaces

The repository has unit, component, API, E2E, frontend, model, schema, type, and
infrastructure checks.

Primary command index: `.mise.toml`.

## Python Backend

Run backend unit tests:

```bash
mise run test:unit
```

This generates protobufs first, then runs:

```bash
uv run python -m pytest backend/ -q --ignore-glob='**/test_*_integration.py'
```

Run backend lint/type/notebook/schema checks:

```bash
mise run lint:python
mise run lint:schemas
```

`mise run lint` includes Python, Terraform, frontend, and schema checks.

## Model Package

Run model package tests:

```bash
mise run test:model
```

This runs from `model/` with extras:

```bash
uv run --extra dev --extra scoring --extra vertex pytest tests/
```

Model unit tests mock GCS and Vertex boundaries. They must not submit paid
Vertex jobs, run Vertex batch inference, execute notebooks, or run full
end-to-end evals.

Notebook checks:

```bash
mise run lint:notebooks
mise run format:notebooks
```

Gemini SFT workflow tests live under `model/tests/gemini_sft`. Shared model
helpers have tests under `model/tests/common/tests`.

## Frontend

Frontend package commands are run with Yarn:

```bash
yarn --cwd frontend/api test
yarn --cwd frontend/transcription-ui test
yarn --cwd frontend/api lint:check
yarn --cwd frontend/transcription-ui lint:check
yarn --cwd frontend/api format:check
yarn --cwd frontend/transcription-ui format:check
yarn --cwd frontend/api typecheck
yarn --cwd frontend/transcription-ui typecheck
```

The aggregate Mise task is:

```bash
mise run lint:frontend
```

The frontend API build also verifies generated routes/OpenAPI output:

```bash
yarn --cwd frontend/api build
yarn --cwd frontend/api verify-spec
```

## Component, API, And E2E Tests

Storage component tests:

```bash
mise run test:component
mise run test:component:rules
mise run test:component:feeds
mise run test:component:audio_segments
```

API integration tests:

```bash
mise run test:api
```

Full Docker Compose E2E:

```bash
mise run test:e2e
```

E2E against an already-running local environment:

```bash
mise run test:e2e:local
```

E2E defaults to mock transcription. To test with local Whisper, set
`TRANSCRIBER_TYPE=local_whisper` and `COMPOSE_PROFILES=local-whisper` in
`local_dev/LOCAL.env`, then run `mise run test:e2e`.

## Terraform

Run Terraform checks:

```bash
mise run lint:terraform
```

CI also validates the AlloyDB module and runs a HOT-protection SQL guard
against disposable Postgres.

## CI

Main CI workflow: `.github/workflows/ci.yml`.

CI has smart routing:

- Backend checks run when non-model or non-Markdown files change.
- Model tests run when `model/` or workflow files change.
- Python quality checks run for backend or model changes.

Major CI jobs:

- Python code quality: `uv sync`, protobuf generation, schema validation,
  Ruff, Ty, notebook checks.
- Model tests: `mise run test:model`.
- TypeScript quality: Yarn install/build, ESLint, Prettier, TSOA routes/spec,
  TypeScript checks.
- Backend tests: pytest with coverage.
- Frontend tests: Vitest with coverage.
- Docker smoke tests for selected backend images/imports/settings.
- Terraform format/validate.
- AlloyDB HOT-protection check.

## Host Stability

For heavy local runs, especially tests, builds, Docker/E2E, browser runs, and
benchmarks, prefer the repository instruction:

```bash
safe-run -- <command> [args...]
```

Docker is already constrained by host configuration, but `safe-run` is useful
for non-Docker or mixed workloads.

## Fast Verification Choices

Use targeted checks when changing a small area:

- Backend service/storage change: relevant `pytest backend/...` plus
  `mise run lint:python` if touching shared code.
- Frontend API/UI change: package test, lint, typecheck, and build/spec if API
  routes changed.
- Model helper/SFT change: `mise run test:model` or focused `pytest` under
  `model/tests`, with `PYTHONPATH=model/src` if invoking tests manually.
- Terraform change: `mise run lint:terraform`.
- Protobuf change: `mise run generate:protos` and schema validation.
