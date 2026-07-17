# Workspace Instructions

> [!IMPORTANT]
> Always read and parse the following project style guides before making code changes or reviewing code in this repository:
> - Python Style: [PYTHON_STYLE.instructions.md](../.github/instructions/PYTHON_STYLE.instructions.md)
> - JS/TS Style: [JS_TS_STYLE.instructions.md](../.github/instructions/JS_TS_STYLE.instructions.md)

Refer to these guidelines to ensure consistency with project standards.

---

## Critical Local Test Safety

This repo has resource-heavy Docker/testcontainers and E2E lanes. Broad local
test commands have previously exhausted developer machines.

Default behavior for agents:

- If the user asks to "run tests" without specifying scope, run the narrowest
  relevant low-resource check.
- For docs-only changes, run `git diff --check`; do not run Python tests unless
  the user asks for them.
- Safe backend default: `uv run python -m pytest backend/ -q --ignore-glob='**/test_*_integration.py'`.
- Use `mise run test` only after inspecting `.mise.toml` and confirming it is
  scoped to safe backend/unit tests.
- Do not proactively run local E2E, API, component, Docker, testcontainers, or
  full integration-stack commands unless the user explicitly asks and confirms
  the machine is prepared. Examples: `mise run test:e2e`,
  `mise run test:e2e:local`, `mise run test:component`, `mise run test:api`,
  `docker compose ... integration-tests`, `uv run pytest integration_tests/`,
  or unscoped `uv run pytest`.
- Prefer GitHub Actions for E2E/resource-stack validation: push the branch,
  check `gh pr checks <pr-number> --repo watch-duty/radio-transcription`, and
  inspect failures with
  `gh run view <run-id> --repo watch-duty/radio-transcription --log`.
- If the user explicitly approves a large local test, run the narrowest path
  possible, keep Docker/testcontainers/API/E2E/component tests serial with
  `-n 0`, and avoid `-n auto` unless xdist fanout is specifically requested.

Reason: local E2E/resource-stack tests can start many containers and emulators,
including AlloyDB Omni, Pub/Sub, GCS, Redis, and pipeline services. CI runners
isolate those resource costs.

## Runtime Validation and Trust Seams

Within statically checked monorepo code, trust annotated parameters and domain
objects whose constructors already enforce their invariants. After verifying
all legitimate callers, do not add runtime type checks that only repeat an
annotation, such as `isinstance()` wrappers around enums, UUIDs, or validated
dataclasses.

- Validate a value once, at the earliest real trust seam.
- Treat an annotation as trusted only when CI's static checker covers both the
  implementation and its legitimate callers. Annotations in excluded or
  untyped subtrees, library or notebook entry points, and untyped callback or
  plugin seams do not by themselves justify removing runtime validation.
- Before adding or removing a runtime check, inspect the repository's callers
  and identify the untrusted input or invariant that the check protects.
- When one condition combines type and domain validation, remove only the
  redundant type arm. Keep semantic subtype exclusions such as rejecting
  `bool` where an integer is required, along with nonempty, range, and
  cross-field checks.
- Keep constructor validation that establishes a trustworthy domain object.
- Keep runtime validation for external or untyped inputs, database rows,
  deserialized data, value ranges, cross-field invariants, canonical identity
  relationships, authorization, ownership, and fencing.
- Keep runtime union or variant discrimination that selects behavior; it is
  dispatch, not duplicate validation.
- Remove tests that use `Any`, casts, or fabricated wrong-type objects only to
  exercise a redundant guard at a trusted seam. Keep tests for real trust seams
  and substantive invariants.
- Do not remove substantive domain or authority validation merely because a
  parameter has a type annotation.

## Agent Action Standards

Prefer the project's **mise** task runner for standard formatting, linting, and
generation tasks. For test execution, follow the Critical Local Test Safety
section above; keep local validation narrow unless the user explicitly asks for
a larger resource-heavy run.

1. **Code Formatting & Linting**:
   - Run `mise run format` locally to automatically format all modified files (Python, Terraform, Frontend) before submitting.
   - Run `mise run lint` locally to execute all code quality checks, formatting linters, and strict type checks.

2. **Protobuf Generation**:
   - Run `mise run generate:protos` to regenerate Python Protobuf/gRPC bindings after making changes to any `.proto` schemas.

3. **Git Commits**:
   - Use descriptive semantic commit prefixes (e.g., `feat(transcription):`, `fix(pipeline):`, `style(transcription):`, `docs:`).
   - **Local Quality Enforcement (Do Not Bypass Hooks)**: With local pre-commit hooks optimized, commits run fast and green. **Do not use `--no-verify`**.
   - **Notebook Schema Gating**: If you modify any `.ipynb` notebook file, you MUST explicitly execute `uv run python scripts/notebook_formatter.py --write` and `uv run ruff format` prior to committing to guarantee schema compliance.

4. **Pull Request Title Standards**:
   - When creating or submitting a Pull Request on GitHub, you MUST prefix the PR title in brackets to satisfy the remote Linear check.
   - **Supported Prefix Formats**:
     - If the work corresponds to a tracked Linear ticket, prefix the PR title with the exact issue ID (e.g. `[GOO-123] feat: implementation...`).
     - For document updates, chores, metadata, or minor changes that do not warrant a ticket, you MUST prefix the PR title with `[ENG-ONLY]` or `[DEV-ONLY]` (e.g., `[ENG-ONLY] docs: update instructions`).
     - Failure to provide one of these prefixes will cause the remote GitHub Actions title validation check to fail immediately.

## Protobuf Python Type Checking Standards

We now maintain official `types-protobuf` stubs in `pyproject.toml` (`[dependency-groups.dev]`)!

1. **Zero Ignore Comments Needed:** You do NOT need to sprinkle `# type: ignore` or `# pyright: ignore` comments on standard `google.protobuf` dynamic imports (like `Duration` or `Timestamp`). Remote CI static type checkers (`ty` / `pyright`) correctly resolve them.
2. **Pristine Environment Harmony:** We maintain `reportUnusedTypeIgnoreComment = false` under `[tool.pyright]` in `pyproject.toml` to ensure absolutely robust, conflict-free local and remote validation.
