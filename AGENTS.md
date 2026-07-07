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

## Enforce Concurrent Index Creation

To prevent database lock contention and connection pool starvation in production:

- **Always** use `CONCURRENTLY` when creating indexes on high-throughput or write-heavy tables (e.g., `annotations`, `audio_segments`, `feeds`).
- Example: `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_name ON table_name ...`
- Since concurrent index builds cannot run inside transaction blocks, ensure they are not wrapped in `BEGIN ... COMMIT` statements.
