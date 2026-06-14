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

## GSD Project Context

This worktree has GSD planning artifacts for the Evidence-Based Quarantine
Policy project:

- [.planning/PROJECT.md](.planning/PROJECT.md) — project context, core value,
  constraints, and decisions.
- [.planning/REQUIREMENTS.md](.planning/REQUIREMENTS.md) — checkable v1
  requirements and traceability.
- [.planning/ROADMAP.md](.planning/ROADMAP.md) — three-phase implementation
  roadmap.
- [.planning/STATE.md](.planning/STATE.md) — current phase and session state.
- [.planning/codebase/](.planning/codebase/) — codebase map.

Before planned implementation work, read `.planning/STATE.md`, then the
relevant project, requirements, roadmap, and codebase-map sections. Prefer GSD
entry points such as `$gsd-plan-phase`, `$gsd-execute-phase`, `$gsd-quick`, or
`$gsd-debug` so planning artifacts and execution context stay in sync.
