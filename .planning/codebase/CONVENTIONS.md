# Coding Conventions

**Analysis Date:** 2026-06-14

## Authoritative Style Guides

- Python: `.github/instructions/PYTHON_STYLE.instructions.md`
- JS/TS: `.github/instructions/JS_TS_STYLE.instructions.md`
- Repo agent instructions: `AGENTS.md` and `.agents/instructions.md`

Read these before code changes or code review. This document summarizes the
patterns observed in the codebase and the most important rules from those
guides.

## Naming Patterns

**Python Files:**
- Use `snake_case.py` for modules.
- Tests use `test_*.py`.
- Internal helpers use a single leading underscore, as in
  `_PipelineFailure` and `_calculate_branch_limits`.

**Python Functions and Variables:**
- Use `snake_case`.
- Constants use `UPPER_SNAKE_CASE`.
- Types/classes use `PascalCase`.
- Enums generally subclass `enum.StrEnum` and expose uppercase member names
  with lowercase string values.

**TypeScript Files:**
- The TS/JS style guide says source filenames should use `snake_case`.
- Existing UI component files commonly use `PascalCase.tsx`; match the local
  directory when modifying existing UI code.
- Existing tests commonly use `*.test.ts` and `*.test.tsx`; the style guide
  says new test names should use `_test.ts` or `_test.tsx`.

**TypeScript Symbols:**
- Use named exports, not default exports.
- Prefer function declarations for named functions.
- Use `const` by default; use `let` only when reassignment is required.
- Prefer `import type` for type-only imports.

## Code Style

**Python Formatting:**
- Ruff is the formatter and linter.
- Target Python version is 3.13.
- Line length is 80.
- Imports are grouped as future, standard library, third-party, then local
  packages.
- Prefer full package imports and avoid relative imports.

**Python Typing:**
- Use modern annotations such as `X | None`.
- `TYPE_CHECKING` imports are common for avoiding runtime import cost.
- TypedDicts model database row shapes in storage code.

**TypeScript Formatting:**
- Prettier and ESLint enforce frontend style.
- Use UTF-8.
- Use modules, not namespaces.
- Do not use `var`.
- Do not use default exports.

## Import Organization

**Python:**
1. `from __future__ import annotations` when needed.
2. Standard library imports.
3. Third-party imports.
4. `backend...` local imports.
5. `TYPE_CHECKING` imports for type-only dependencies.

**TypeScript:**
1. External packages.
2. Internal package imports such as `@transcription/common`.
3. Relative imports.
4. Type-only imports where applicable.

The frontend uses sort-import tooling through Prettier and ESLint tasks.

## Error Handling

**Ingestion Runtime:**
- Collectors should raise typed `FeedFailure` for known feed-level evidence.
- Runtime raises private `_PipelineFailure` for post-capture side effects.
- Unexpected exceptions are captured at runtime boundaries and recorded with
  defensive status reasons.
- Fencing failures on bookmark writes terminate the process intentionally
  through `os._exit(1)` after logging and setting lease-lost state.

**Storage/API:**
- Storage methods return booleans or typed dicts for expected state outcomes.
- Service layers convert invalid IDs into `None` or HTTP 404/400 responses.
- FastAPI routes translate domain/storage exceptions into `HTTPException`.

**Python Style Guide:**
- Use built-in exceptions where possible.
- Avoid catch-all `except:` and catch `Exception` only at isolation points.
- Keep `try` blocks small.

## Logging

**Python:**
- Use standard `logging`.
- Structured operational logs pass `extra={"json_fields": {...}}`.
- Runtime SLO and quarantine events use stable `event_type` strings.
- Avoid logging secrets, URLs with credentials, signed URLs, request bodies, or
  high-cardinality raw payloads.

**Frontend API:**
- Express error middleware logs errors and returns a consistent JSON shape.
- Avoid ad hoc console output outside boundary/error handling.

## Comments and Docs

**Python:**
- Use triple-quoted docstrings.
- Public APIs and non-obvious logic should have docstrings with `Args:`,
  `Returns:`, and `Raises:` when applicable.
- Comments should explain why a constraint exists, not restate obvious code.
- TODO format is `# TODO: reference - explanatory string`.

**Repo Pattern:**
- The ingestion runtime uses detailed comments for concurrency, lease, and
  failure-handling invariants. Preserve that level of explanation when changing
  those invariants.
- Collector docs should be updated when behavior changes would make
  `backend/pipeline/ingestion/collectors/README.md` misleading.

## Function and Module Design

**Python:**
- Prefer small helpers for testable policy and allocation logic.
- Avoid mutable globals unless clearly internal and justified.
- Do not rely on built-in atomicity for threaded coordination; the runtime uses
  `threading.Event` for cross-thread state.
- Use dataclasses and TypedDicts for clear boundary models.

**TypeScript:**
- Prefer named exports.
- Do not create container classes for namespacing.
- Use readonly class fields when they are not reassigned.
- Avoid deep object destructuring in parameters.

## Domain-Specific Conventions

**Feed Failure Classification:**
- `status_reason` is canonical abnormal-condition state.
- `quarantine_reason` is raw forensic detail and should not drive policy.
- Reason strings must be bounded and safe for operator surfaces.
- Item failures should stay item-scoped unless an observation boundary fails
  completely.

**Collector Contract:**
- Collectors must not write database state.
- Collectors own source retry/backoff and source-specific classification.
- Runtime owns GCS upload, Pub/Sub publish, bookmarks, leases, heartbeats,
  retries after failure, and quarantine telemetry.
- `SourceObservation` is success without audio and can clear stale failure
  state.

**Testing Discipline:**
- Keep tests focused and local to changed behavior.
- Avoid broad local test suites unless explicitly requested.
- For docs-only changes, use `git diff --check` rather than Python tests.

---

*Convention analysis: 2026-06-14*
*Update when patterns change*
