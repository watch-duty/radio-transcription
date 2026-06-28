# Agent Instructions

Read and follow [.agents/instructions.md](.agents/instructions.md) before
making code changes or reviewing code in this repository.

## Do Not Run Broad Local Tests By Default

This repository has resource-heavy Docker/testcontainers and E2E lanes. Broad
local test commands have previously exhausted developer machines.

- Default to targeted low-resource checks locally.
- For docs-only changes, use `git diff --check` instead of Python tests unless
  the user asks for tests.
- Do not run local E2E/API/component/full integration tests unless the user
  explicitly asks and confirms the machine is prepared.
- Avoid unscoped `uv run pytest`, `uv run pytest integration_tests/`,
  `mise run test:e2e`, `mise run test:component`, and
  `docker compose ... integration-tests` unless explicitly approved.
- Prefer GitHub Actions for full E2E/resource-stack validation.

<!-- GSD:project-start source:PROJECT.md -->
## Project

**Gemini SFT Workflow Onboarding**

This project makes the radio-transcription Gemini SFT and evaluation workflow
usable by someone who has not followed the prior experiment thread. It turns
the existing notebooks, scripts, SFT CLI, checkpoint scorer, manifests, and
GCS artifact conventions into a config-driven workflow with clear reports and
minimal hidden context.

The audience is Watch Duty engineers and researchers running prior-context
Gemini transcription experiments, comparing checkpoints, and deciding which
fine-tuned model or prompt setup should be promoted.

**Core Value:** A new operator can run and compare Gemini SFT/eval experiments from explicit
configs and console reports without reverse-engineering notebooks or prior
chat history.

### Constraints

- **Runtime**: Keep SFT/eval runnable from the lightweight ASR operator
  runtime described in `ASR_CONTRIBUTING.md` - this avoids NeMo/GPU overhead
  for Gemini work.
- **Durability**: GCS run prefixes and normalized inference manifests must be
  sufficient to resume, inspect, and compare runs - local `results/` is only a
  mirror/cache.
- **Prompt parity**: Prompt and request construction must come from shared
  helpers so notebooks, SFT JSONL, batch eval, and checkpoint scoring do not
  drift.
- **Prior context**: Prior transcripts are computed at run time from same
  original audio/session ordering; they are not static manifest fields.
- **Metrics**: Reports must distinguish exact empty responses from the
  historical empty/unintelligible rate.
- **Cost**: Vertex tuning and inference can spend money; paid operations need
  explicit commands, clear output locations, and resumable state.
- **Git hygiene**: Do not commit local experiment TOMLs, raw prediction JSONL,
  or `results/` outputs unless explicitly requested.
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

- Backend services target Python 3.13 and are managed by the root `uv`
  workspace. Service packages live under `backend/pipeline/*` and
  `backend/services/*`.
- Model and ASR research code targets Python 3.11 through the separate
  `model/pyproject.toml` package. The `gemini-sft` CLI is exposed from
  `model/src/gemini_sft`.
- Frontend packages target Node.js 22 and Yarn. The frontend is split into
  `frontend/common`, `frontend/api`, and `frontend/transcription-ui`.
- Local orchestration uses Docker Compose. Standard commands are in
  `.mise.toml`; prefer `mise` tasks for routine development commands.
- Cloud runtime is Google Cloud: Pub/Sub, Cloud Storage, Cloud Run/Functions
  style services, AlloyDB/Postgres, Memorystore/Redis, Vertex AI/Gemini, and
  Google auth.
- Gemini SFT/eval work should use the lightweight ASR Docker runtime from
  `asr-eval-docker-compose.yml`, especially `notebooks-cpu`, which installs
  `/workspace/model[scoring,vertex]` editable.
- Keep new SFT/eval work inside the existing `model/` package and
  `gemini-sft` workflow unless there is a concrete reason to expand scope.
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

- Use `CONTEXT.md` for domain terminology.
- Use `.mise.toml` for local task names and standard development commands.
- Use root `pyproject.toml` for backend Python package, lint, type, and pytest
  configuration.
- Use `model/pyproject.toml` for model package dependencies and the
  `gemini-sft` console entry point.
- Use `model/scripts/sft/README.md` for Gemini SFT workflow semantics.
- Use GCS `config.json` under `gs://<bucket>/sft/runs/<round-id>/` as the
  durable SFT/eval run state.
- Prefer full package imports over relative imports.
- Use built-in exception classes and avoid catch-all exception handling except
  at isolation boundaries.
- Use 4-space indentation and 80-character line length.
- Use Google-style docstrings where docstrings are needed.
- Group imports as future, standard library, third-party, local.
- Keep public APIs typed and avoid `assert` for application logic.
- Prefer named exports; avoid default exports.
- Use `const` by default and `let` only when reassignment is needed.
- Prefer relative imports within the same logical project.
- Keep frontend files formatted by Prettier and linted by ESLint.
- API route/spec generation is part of the frontend API build contract.
- Do not commit real Gemini SFT run configs; commit examples/placeholders only.
- Keep Gemini prompts and requests centralized in
  `model/src/common/gemini/prompts.py`, `context.py`, `vertex.py`, and
  `tuning_data.py`.
- Report exact empty responses separately from the historical
  empty-or-unintelligible rate.
- Do not commit local `results/`, local SFT configs, downloaded/generated
  inference manifests, or raw prediction JSONL unless explicitly requested.
- Do not overwrite or revert unrelated dirty worktree files.
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

- The repo has four major subsystems: backend audio pipeline, backend domain
  APIs/storage, frontend proxy/UI, and model research/SFT tooling.
- The audio pipeline is event-driven and claim-check based: ingestion captures
  source audio, segmentation stitches transmissions, normalization writes
  canonical/playback/transcription audio, transcription emits transcripts,
  rules evaluation annotates and alerts, and notification sends alerts.
- Domain APIs are FastAPI services over storage stores:
  `FeedService`/`FeedStore`, `AudioSegmentService`/`AudioSegmentStore`, and
  `AlloyRulesService`/`RulesStore`.
- The frontend has an Express BFF in `frontend/api`, shared TypeScript in
  `frontend/common`, and the React/Vite/MUI app in `frontend/transcription-ui`.
- Model and SFT work is packaged under `model/src`. Shared helpers belong in
  `common` and `common.gemini`; workflow orchestration belongs in
  `gemini_sft`.
- Gemini SFT state is GCS-authoritative. `prepare` builds Gemini JSONL from
  canonical manifests, `tune` submits/resumes Vertex tuning, and `eval` runs
  inference/scoring from the durable config.
- Prior-context construction should remain in `common.gemini.context`; request
  construction and Vertex settings should remain in `common.gemini.vertex`.
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->

<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
