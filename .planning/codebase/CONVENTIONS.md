# Coding Conventions

**Analysis Date:** 2026-04-21

Python backend (primary) + TypeScript/React frontend. Configuration is authoritative — ruff's `ALL` rule set with targeted ignores in `pyproject.toml` is the source of truth for Python style. Prettier + ESLint govern frontend.

## Naming Patterns

**Files:**
- Python modules: `snake_case.py` (e.g., `backend/pipeline/ingestion/normalizer_runtime.py`, `quarantine_telemetry.py`, `feed_store.py`).
- Python test modules: prefix `test_<target>.py` co-located under `tests/` (e.g., `backend/pipeline/ingestion/tests/test_retry.py`).
- TypeScript React components: `PascalCase.tsx` (e.g., `frontend/transcription-ui/src/components/audio/AudioPlayer.tsx`).
- TypeScript utilities/services: `camelCase.ts` (e.g., `frontend/transcription-ui/src/service/listFeeds.ts`).
- TypeScript tests: sibling `<Name>.test.tsx` / `<name>.test.ts` next to source (co-located, no `__tests__/` folder).
- Private Python module members: leading underscore (e.g., `_COLLECTORS`, `_client`, `_require_env`, `_make_feed`, `_mock_pubsub_publish`).

**Functions:**
- Python: `snake_case` for both sync and async (`route_capturer`, `retry_with_lease_check`, `emit_quarantine_event`). Async functions use `async def` explicitly — no prefix naming convention.
- Python private/helper: leading underscore (`_dummy_capture`, `_make_feed`, `_make_captured_chunk`, `_make_settings` in `backend/pipeline/ingestion/tests/test_runtime.py:28-118`).
- TypeScript: `camelCase` (`listFeeds`, `renderWithQueryClient` in `frontend/transcription-ui/src/test/testUtils.tsx:4`).
- React components: `PascalCase` (`AudioPlayer`, `DateTimePicker`, `TranscriptView`).

**Variables:**
- Python: `snake_case` for locals and module-level (`logger`, `last_exception`, `feed_id`, `mock_client`).
- Python constants / registries: `UPPER_SNAKE_CASE` (`BCFY_FEEDS_URL_BASE`, `OPENMHZ_URL_BASE`, `_METRIC_TYPE`, `CHUNK_DURATION_SECONDS`) in `backend/pipeline/ingestion/router.py:22-24`.
- Python test module-level constants: `_UPPER_SNAKE_CASE` with leading underscore (`_WORKER_ID`, `_FEED_ID`, `_FEED`, `_LEASE_ROW`) in `backend/pipeline/ingestion/tests/test_runtime.py:24-46`.
- TypeScript: `camelCase`; constants that are exported as `const` also `camelCase` by convention.

**Types:**
- Python classes: `PascalCase` (`NormalizerRuntime`, `CapturedChunk`, `FeedStore`, `LeasedFeed`, `HealthState`, `LeaseExpiredError`).
- Python frozen dataclasses for contract types: `@dataclasses.dataclass(frozen=True)` (`CapturedChunk` in `backend/pipeline/ingestion/models.py:75`).
- Python `kw_only=True` dataclasses for settings: `@dataclass(frozen=True, kw_only=True)` (`NormalizerSettings` in `backend/pipeline/ingestion/settings.py:19`).
- Exception classes: `<Noun>Error` — ruff `N818` is explicitly ignored in `pyproject.toml:124`, so `LeaseExpiredError` is accepted; some project-specific names omit `Error` suffix by convention.
- TypeScript: `PascalCase` for interfaces, types, and enums.

## Code Style

**Formatting:**
- Python: **ruff format** (`.pre-commit-config.yaml:16-20`, `uv run ruff format`). Target Python `py313`. `line-length = 80` (`pyproject.toml:76`).
- TypeScript / React / CSS: **Prettier** 3.8.1. Config in `.prettierrc`: `semi: true`, `trailingComma: "es5"`, `singleQuote: true`, `printWidth: 80`, `tabWidth: 2`.
- Import sorting via `@trivago/prettier-plugin-sort-imports` (`.prettierrc:7-8`) with per-scope `importOrder` arrays for `frontend/api/**` and `frontend/transcription-ui/**`.

**Linting:**
- Python: **ruff** with `select = ["ALL"]` and an explicit ignore list in `pyproject.toml:78-163`. Notable kept-strict rules: no wildcard imports, no shadowed builtins, structured error handling.
- Notable ignored rules (intentional project posture): `E501` (line too long — format handles it), `G004` (f-strings in logging allowed), `PT009` / `PT027` (unittest-style assertions and `assertRaises` are preferred over pytest), `TID252` (parent-relative imports allowed), `UP007` (no PEP 604 `X | Y` requirement).
- `pyproject.toml:167-169`: `max-complexity = 10` (McCabe).
- `pyproject.toml:174-213`: relaxed rules for `**/test*.py` and `model/data/**.py` — missing annotations, mixed-case names, `assert`, and SQL-string construction all permitted in tests.
- Type checking: **`ty`** (`uv run ty check`, `.pre-commit-config.yaml:21-27`), pyright/ty target `python 3.13`. `model/` excluded.
- TypeScript: **ESLint** 10 flat config in `frontend/transcription-ui/eslint.config.js`: `typescript-eslint recommended`, `@tanstack/eslint-plugin-query/flat/recommended`, `react-hooks flat recommended`, `react-refresh/vite`, `@eslint/css`, Prettier shim last. Frontend `dist` globally ignored.

## Import Organization

**Order (Python):**
1. `from __future__ import annotations` — present at the top of 31 backend modules sampled; required for forward references under Python 3.13 without eager evaluation. Required in all new backend modules.
2. Standard library (`asyncio`, `logging`, `unittest`, `uuid`, `datetime`).
3. Third-party (`aiohttp`, `asyncpg`, `pytest`, `pydantic`, `testcontainers`).
4. First-party (`backend.*`, `local_dev`) — declared via `pyproject.toml:171-172` `[tool.ruff.lint.isort] known-first-party = ["backend", "local_dev"]`.
5. `TYPE_CHECKING` imports guarded by `if TYPE_CHECKING:` block (see `backend/pipeline/ingestion/models.py:67-72` and `retry.py:7-9`) to keep runtime import graph minimal.

Ruff / isort re-orders automatically via pre-commit.

**Order (TypeScript — `frontend/transcription-ui`):**
`.prettierrc:31-45`:
1. `^react` (React and React-DOM)
2. `<THIRD_PARTY_MODULES>`
3. `^@` (scoped packages and aliases)
4. `^[./].*(?<!\.(css))$` (relative imports, non-CSS)
5. `\.s?css$` (stylesheets last)

Groups are separated by a blank line (`importOrderSeparation: true`).

**Order (TypeScript — `frontend/api`):**
`.prettierrc:14-29`: `^node:` → `^@google-cloud/` → `^express$` → third-party → `^\./` → `^\.\./`.

**Path Aliases:**
- Python: first-party namespace `backend.*` (absolute imports only; relative parent imports allowed by `TID252` ignore but not the dominant style).
- TypeScript: `@transcription/common` workspace package (`frontend/transcription-ui/package.json:27`), resolved via `file:../common`. No tsconfig `paths` aliases.

## Error Handling

**Patterns:**
- **Custom exception classes** for control-flow-critical signals. Example: `LeaseExpiredError(Exception)` in `backend/pipeline/ingestion/retry.py:14-16` is a sentinel the runtime watches for to distinguish "retry exhausted" from "lease revoked."
- **Lease-aware retry wrapper** `retry_with_lease_check` in `backend/pipeline/ingestion/retry.py:18-124` is the canonical backoff helper for GCS / AlloyDB ops. It:
  - Checks `lease_lost` event before each attempt and raises `LeaseExpiredError`.
  - Checks `shutdown` event and raises `asyncio.CancelledError`.
  - Retries only if the raised exception is in the `retryable` tuple; re-raises non-retryable immediately.
  - Backoff delay races against `lease_lost` and `shutdown` so SIGTERM is never blocked by a sleep.
- **Hard-fail on fence violations**: the runtime calls `os._exit(1)` (not `sys.exit`) on lost lease so in-flight async tasks cannot race further writes. See test case `backend/pipeline/ingestion/tests/test_runtime.py:233-255` (`TestProcessFeedFenceViolation`).
- **Never-raises telemetry functions**: `emit_quarantine_event` in `backend/pipeline/ingestion/quarantine_telemetry.py:37-78` wraps every emission site in `try/except` with `# noqa: S110` where swallowing is intentional. Telemetry must never break the hot path.
- **Explicit per-error-class routing** in collectors. The capture contract in `backend/pipeline/ingestion/models.py:19-58` prescribes six error categories (configuration / rate-limit / transient / item-level / data-quality / unknown) and tells authors exactly which to raise, skip, or swallow.
- **`raise` without `from`** is the house style — `B904` is in the ruff ignore list (`pyproject.toml:88`). Do not add `from err` unless deliberately re-wrapping.
- **No broad `except` warning suppression for defensive catches**: `BLE001` ignored (`pyproject.toml:89`) so `except Exception:` is allowed in infrastructure code, but in business logic prefer the narrowest `except (OSError, asyncpg.PostgresError)` tuple possible (see `retry_with_lease_check`'s `retryable` parameter).

## Logging

**Framework:** Python stdlib `logging`. No third-party logger wrapper.

**Patterns:**
- Every module declares `logger = logging.getLogger(__name__)` at module top — e.g., `backend/pipeline/ingestion/retry.py:11`, `quarantine_telemetry.py:17`, `normalizer_runtime.py:42`.
- **Structured logging** uses `extra={...}` for machine-queryable fields. Canonical example from `backend/pipeline/ingestion/quarantine_telemetry.py:49-57`:
  ```python
  logger.error(
      "Feed quarantined",
      extra={
          "event_type": "feed_quarantined",
          "feed_id": feed_id,
          "feed_name": feed_name,
          "source_type": source_type,
      },
  )
  ```
  Always include an `event_type` key for log routing; everything else is context.
- **`%`-style lazy formatting** is used in paths that may be debug-gated: `logger.warning("%s failed after %d attempts: %s", operation_name, attempt + 1, exc)` in `retry.py:77-82`. `G001` and `G004` are both ignored, so f-strings in log calls are also allowed — use whichever is clearer.
- Log levels by purpose: `error` for terminal / recoverable-but-notable failures (quarantine, fence violation); `warning` for retried-and-recovered issues; `info` for lifecycle and retry attempts; `debug` for hot-path tracing.
- Defensive `try/except` around log calls in never-raise telemetry paths (`quarantine_telemetry.py:58-59, 77-78`) — swallow with `# noqa: S110`.

## Comments

**When to Comment:**
- Module-level docstrings explain the role of the module within the pipeline; see `backend/pipeline/ingestion/models.py:1-60` (full contract spec) and `backend/pipeline/ingestion/quarantine_telemetry.py:1-9`.
- Function docstrings are Google-style (`pyproject.toml:215-216`: `[tool.ruff.lint.pydocstyle] convention = "google"`). `D100`-`D107` are ignored, so docstrings are optional for public modules/classes/functions — but complex behavioral contracts (retry semantics, contract boundaries) MUST be documented.
- Inline comments explain **WHY**, never WHAT. Examples:
  - `retry.py:98-99`: "Race backoff against both lease_lost and shutdown to maintain the runtime's SIGTERM-interruptibility invariant."
  - `settings.py:136-144`: multi-line comment explains why `HEALTH_CHECK_PORT` MUST NOT be overridden in production — production deployment hardcodes 8080.
- `# noqa: <CODE>` markers include the rule being suppressed AND (preferably) a comment explaining why the suppression is load-bearing.

**JSDoc/TSDoc:**
- Not a pervasive pattern in frontend code. Frontend types are TypeScript-annotated rather than JSDoc-annotated.

## Function Design

**Size:**
- McCabe complexity cap of 10 (`pyproject.toml:167-169`). `C901` (function too complex) is ignored, but hitting `max-complexity` still surfaces via `PLR0912`/`PLR0915`-family rules.
- Target: single responsibility per function. `retry.py:retry_with_lease_check` is the ceiling — it's ~100 LOC and does orchestrate retry+backoff+interrupt handling as a single logical unit. If a new function grows beyond that, split.

**Parameters:**
- `PLR0913` (too many arguments) is ignored — long `NormalizerSettings`-style constructors are acceptable.
- **Keyword-only arguments** for constructors/settings dataclasses (`kw_only=True` in `backend/pipeline/ingestion/settings.py:19`). For functions, require keyword arguments via `*,` when there are more than 2–3 positional args or when correctness depends on named call sites (see `retry_with_lease_check`'s `lease_lost`, `shutdown` keyword-only args).
- Default factories via `field(default_factory=lambda: ...)` pull from env vars in settings dataclasses.

**Return Values:**
- Prefer explicit return types on non-test public functions (`ANN001`-`ANN206` relaxed only inside `**/test*.py` — `pyproject.toml:175-197`).
- `TypedDict` (e.g., `LeasedFeed`) used where the caller needs dict-literal ergonomics but typed access.
- Frozen dataclasses for value objects that cross module boundaries (`CapturedChunk` is the canonical example).

## Module Design

**Exports:**
- No `__all__` anywhere in the backend — importers reach directly into modules. If something is importable, it is part of the implicit public API; prefix with `_` to mark private.
- Frozen dataclasses and TypedDicts are imported by name from their defining module (e.g., `from backend.pipeline.ingestion.models import CapturedChunk`).

**Barrel Files:**
- Not used in the Python backend. `__init__.py` files are present for package declaration only (mostly empty) — no re-exports.
- In the frontend, deep imports are preferred (`from './listFeeds'`), and only the shared workspace package `@transcription/common` has an `index` entry point.

---

*Convention analysis: 2026-04-21*
