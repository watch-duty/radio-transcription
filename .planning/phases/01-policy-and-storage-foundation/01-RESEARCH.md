# Phase 1: Policy And Storage Foundation - Research

**Researched:** 2026-06-15
**Domain:** Python ingestion policy contract, async feed lifecycle storage, and focused pytest validation
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

Source: [VERIFIED: .planning/phases/01-policy-and-storage-foundation/01-CONTEXT.md]

### Locked Decisions

### Policy Evidence Contract

- **D-01:** Create `backend/pipeline/ingestion/failure_policy.py` for policy
  vocabulary and pure policy logic. Do not keep these policy enums/classes in
  `models.py` long term.
- **D-02:** `failure_policy.py` owns `OwnerScope`, `FailureScope`,
  `EndpointKind`, `PolicyIntent`, `ExecutedAction`, `PipelineStage`,
  `FailurePolicyEvidence`, `FailurePolicyDecision`,
  `classify_failure_policy(status_reason, evidence)`, and pure predicates such
  as `is_feed_quarantine`, `is_feed_budget_eligible`, `is_pipeline_hold`, and
  `is_source_class_breaker`.
- **D-03:** `failure_policy.py` must not own runtime side effects. The runtime,
  stores, publisher helpers, breaker stores, hold/replay stores, telemetry, and
  alerting remain outside the policy module.
- **D-04:** `FailurePolicyEvidence` contains facts only: ownership, scope,
  endpoint, and optional pipeline stage. It must not contain the final policy
  verdict. `PolicyIntent`, `ExecutedAction`, and budget/quarantine booleans
  belong on `FailurePolicyDecision`.
- **D-05:** Typed `FeedFailure` is strict. It must require
  `policy_evidence`. There is no intentional known/classified failure path
  without policy evidence.
- **D-06:** Untyped runtime exceptions are not `FeedFailure`. Runtime may
  synthesize `UNKNOWN` / telemetry-gap policy evidence for those defensive
  fallback paths.
- **D-07:** Do not add `reason_family` in Phase 1. V1 routing can be decided
  from `status_reason` plus `owner_scope`, `failure_scope`, `endpoint_kind`,
  and `pipeline_stage`. Raw `reason` remains forensic detail only.
- **D-08:** Collector/source-specific code owns raw signal extraction and
  construction of typed failure evidence. Runtime owns execution of policy
  decisions. Storage owns DB state transitions.

### Non-Budgeted Storage Semantics

- **D-09:** Keep `failure_count` in v1, but shrink its meaning to
  "consecutive feed-budget-eligible failures only."
- **D-10:** Non-budgeted failure storage always sets `failure_count = 0`.
  Non-budgeted observations are not part of any feed quarantine episode and
  must clear old mixed-budget debt.
- **D-11:** `release_non_budgeted_failure(...)` writes `status='failing'`,
  `failure_count=0`, `retry_after`, and `status_reason`.
- **D-12:** `release_non_budgeted_failure(...)` releases the active lease
  (`worker_id = NULL`) and preserves scheduler metadata needed by the existing
  failing/recovery path, such as `unclaimed_since` if required by local SQL
  conventions.
- **D-13:** `release_non_budgeted_failure(...)` must never write
  `quarantine_reason`. Reset/progress flows own clearing quarantine-specific
  forensic data.
- **D-14:** `report_feed_failure(...)` remains the only storage path that
  increments the feed quarantine budget.
- **D-15:** Existing successful progress and `SourceObservation` stale-state
  clearing semantics must remain intact.

### Status Reason Vocabulary

- **D-16:** Add only status enum values needed by current code paths. Do not
  add speculative status reasons.
- **D-17:** For v1, add `pipeline_publish_after_bookmark_failed` because it has
  distinct semantics: capture/bookmark advanced, publish failed, a downstream
  data gap is known, and v1 has no replay.
- **D-18:** Allow the `pipeline_` prefix in `FeedStatusReason`, but keep it
  rare. `pipeline_*` means downstream post-capture consistency/replay
  semantics, not simply "code lives in the pipeline."
- **D-19:** `pipeline_*` status reasons must never increment feed budget and
  must never quarantine the feed.
- **D-20:** Status reason prefixes are operator taxonomy, not routing
  authority. Budget eligibility is decided by `FailurePolicyDecision`, not by
  `source_` / `system_` / `pipeline_` string prefixes.
- **D-21:** Do not rename `FeedStatusReason` in v1. The compatibility cost is
  higher than the benefit; document that it is an abnormal ingestion/feed
  processing reason, not necessarily feed health.

### the agent's Discretion

The agent may choose exact enum member names and helper function names within
the locked ownership boundary, as long as tests enforce that typed
`FeedFailure` requires evidence and that `pipeline_*` decisions cannot
increment feed budget.

### Deferred Ideas (OUT OF SCOPE)

- Durable publish outbox / hold-replay worker belongs to a later phase.
- Source-class / credential breaker persistence belongs to a later phase.
- Persistent structured policy audit table belongs to a later phase.
- Renaming `FeedStatusReason` can be reconsidered later if the compatibility
  surface is worth the churn.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| POL-01 | Runtime failure routing uses structured policy evidence fields rather than `quarantine_reason` or raw reason text for quarantine and alert decisions. | Use `failure_policy.py` as the pure policy boundary and keep raw `reason`/`quarantine_reason` forensic only. [VERIFIED: 01-CONTEXT.md; .planning/REQUIREMENTS.md; backend/pipeline/ingestion/collectors/README.md] |
| POL-02 | The policy evidence model includes `owner_scope`, `failure_scope`, `endpoint_kind`, `policy_intent`, and `executed_action`. | Interpret through D-04: facts live on `FailurePolicyEvidence`; `policy_intent` and `executed_action` live on `FailurePolicyDecision`, so the overall policy contract covers the requirement without putting verdict fields on evidence. [VERIFIED: 01-CONTEXT.md; .planning/REQUIREMENTS.md] |
| POL-03 | The policy evidence model includes pipeline stage detail for pipeline-owned failures. | Include optional `PipelineStage` on `FailurePolicyEvidence` and test pipeline-owned decisions such as post-bookmark Pub/Sub publish failure. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/collector_runtime.py] |
| STORE-01 | Storage exposes a non-budgeted failure release method that releases the lease and writes `status='failing'`. | Add/verify `FeedStore.release_non_budgeted_failure(...)` backed by fenced SQL that sets `worker_id = NULL` and `status = 'failing'`. [VERIFIED: backend/pipeline/storage/feed_store.py; backend/pipeline/storage/feed_queries.py] |
| STORE-02 | The non-budgeted failure release method always writes `failure_count=0`. | SQL must set `failure_count = 0`, not increment or preserve old debt. [VERIFIED: 01-CONTEXT.md; backend/pipeline/storage/feed_queries.py] |
| STORE-03 | The non-budgeted failure release method writes `retry_after` and `status_reason`. | Method signature should require `retry_after` and `FeedStatusReason`; SQL parameters should write both. [VERIFIED: backend/pipeline/storage/feed_store.py; backend/pipeline/storage/feed_queries.py] |
| STORE-04 | The non-budgeted failure release method never writes `quarantine_reason`. | Keep `quarantine_reason` absent from non-budgeted SQL; only reset/progress/quarantine-specific flows should touch it. [VERIFIED: 01-CONTEXT.md; backend/pipeline/storage/feed_queries.py] |
| STORE-05 | `report_feed_failure(...)` remains the only path that increments the feed quarantine budget. | Keep `failure_count = failure_count + 1` only in `REPORT_FAILURE_SQL`; non-budgeted SQL must reset to zero. [VERIFIED: backend/pipeline/storage/feed_queries.py] |
| STORE-06 | Successful chunk progress and `SourceObservation` continue to clear stale failure count and status reason state. | Preserve `UPDATE_PROGRESS_SQL` and `RECORD_SOURCE_OBSERVATION_SQL` clearing behavior. [VERIFIED: backend/pipeline/storage/feed_queries.py; backend/pipeline/storage/feed_store.py] |
| STAT-01 | Backend status reason enum includes `pipeline_publish_after_bookmark_failed`. | Add/verify `FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED` and status-row parsing tests. [VERIFIED: backend/pipeline/storage/feed_store.py; backend/pipeline/storage/tests/test_feed_store.py] |
</phase_requirements>

## Summary

Phase 1 should be planned as a narrow backend foundation change: add a pure policy module, make typed `FeedFailure` evidence strict, add/verify one non-budgeted storage method, and preserve existing progress/observation recovery semantics. [VERIFIED: .planning/ROADMAP.md; 01-CONTEXT.md] The planner should not add a database migration, new lifecycle status, durable replay, breaker persistence, or audit table because the v1 roadmap explicitly excludes those surfaces. [VERIFIED: .planning/PROJECT.md; .planning/REQUIREMENTS.md; .planning/ROADMAP.md]

The biggest planning caveat is current dirty state. `git status --short` shows uncommitted edits in ingestion, storage, and tests, including files Phase 1 will touch. [VERIFIED: git status --short] Current code already has policy enums and `FailurePolicyEvidence` in `backend/pipeline/ingestion/models.py`, while `backend/pipeline/ingestion/failure_policy.py` is absent; that conflicts with locked decisions D-01 and D-02. [VERIFIED: backend/pipeline/ingestion/models.py; file existence check; 01-CONTEXT.md] Current `FeedFailure` still allows `policy_evidence=None`, while D-05 requires strict evidence for typed failures. [VERIFIED: backend/pipeline/ingestion/models.py; 01-CONTEXT.md]

**Primary recommendation:** Plan Wave 0 as a reconciliation step: move policy vocabulary/logic out of `models.py` into `failure_policy.py`, split facts from verdicts with `FailurePolicyDecision`, update strict `FeedFailure` tests, then verify the storage SQL/method and status reason primitives without broad runtime routing. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/models.py; backend/pipeline/storage/feed_queries.py]

## Project Constraints (from AGENTS.md)

- The workspace has an `AGENTS.md` that requires reading `.agents/instructions.md` before code changes or code review. [VERIFIED: AGENTS.md]
- Broad local tests are forbidden by default because local Docker/testcontainers/E2E lanes are resource-heavy. [VERIFIED: AGENTS.md; .agents/instructions.md]
- For docs-only changes, use `git diff --check` rather than Python tests unless the user asks for tests. [VERIFIED: AGENTS.md; .agents/instructions.md]
- Agent-run tests, builds, installs, browser/e2e runs, benchmarks, and stress tests should be wrapped with `safe-run --`. [VERIFIED: prompt AGENTS.md instructions; AGENTS.md; .agents/instructions.md]
- Local E2E/API/component/full integration tests require explicit user approval and machine-readiness confirmation. [VERIFIED: AGENTS.md; .agents/instructions.md]
- Standard formatting/lint/generation tasks should prefer `mise`; protobuf changes require `mise run generate:protos`. [VERIFIED: .agents/instructions.md]
- Commits must not use `--no-verify`, and PR titles require `[GOO-123]`, `[ENG-ONLY]`, or `[DEV-ONLY]` prefixes. [VERIFIED: .agents/instructions.md]
- Project-specific skills were not found under `.codex/skills/` or `.agents/skills/` in this worktree. [VERIFIED: project skills discovery command]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Policy vocabulary and pure decision classification | Backend ingestion domain module | Collector Runtime | Policy decisions are backend domain rules and must be side-effect-free; runtime executes decisions later. [VERIFIED: 01-CONTEXT.md] |
| Collector construction of typed failure evidence | Source Collector Layer | Backend ingestion domain module | Collectors own source-specific signal extraction before crossing the runtime boundary. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/collectors/README.md] |
| Runtime execution of policy decisions | Collector Runtime Layer | Storage Layer | Runtime owns leases, telemetry, and side effects; storage owns persisted transitions. [VERIFIED: .planning/codebase/ARCHITECTURE.md; 01-CONTEXT.md] |
| Non-budgeted suppressed retry persistence | Storage Layer | Collector Runtime Layer | `FeedStore`/SQL owns DB state, while runtime supplies feed, lease, retry, and status inputs. [VERIFIED: backend/pipeline/storage/feed_store.py; backend/pipeline/storage/feed_queries.py] |
| Status reason vocabulary | Storage/domain model | API/UI later phases | Backend `FeedStatusReason` maps text values from `feeds.status_reason`; UI compatibility is deferred to Phase 3. [VERIFIED: backend/pipeline/storage/feed_store.py; .planning/ROADMAP.md] |
| Progress and `SourceObservation` recovery semantics | Storage Layer | Collector Runtime Layer | SQL clears stale failure state; runtime only calls the store after successful audio progress or source observation. [VERIFIED: backend/pipeline/storage/feed_queries.py; backend/pipeline/storage/feed_store.py] |

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| Python | Project requires `>=3.13,<3.14`; uv-managed CPython 3.13.12 is installed | Backend policy dataclasses/enums and storage code | Matches repository package requirement and Ruff target. [VERIFIED: pyproject.toml; uv python list --only-installed] |
| Python stdlib `enum.StrEnum` and `dataclasses` | Python 3.13 stdlib | Policy vocabulary and immutable value objects | Existing code already uses `StrEnum` and dataclasses for policy-like boundary models. [VERIFIED: backend/pipeline/ingestion/models.py; backend/pipeline/storage/feed_store.py] |
| asyncpg | 0.31.0 locked | Async PostgreSQL/AlloyDB access for `FeedStore` | Existing storage facade uses asyncpg-style `fetchrow`/`execute` with parameterized SQL constants. [VERIFIED: uv.lock; backend/pipeline/storage/feed_store.py] |
| pytest | 9.0.3 locked | Focused unit/storage/runtime tests | Existing backend tests use unittest classes under pytest and are configured in `pyproject.toml`. [VERIFIED: uv.lock; pyproject.toml; backend/pipeline/storage/tests/test_feed_store.py] |
| pytest-asyncio | 1.3.0 locked | Async test support | Existing async storage/runtime tests use `unittest.IsolatedAsyncioTestCase` under pytest with asyncio config enabled. [VERIFIED: uv.lock; pyproject.toml; backend/pipeline/storage/tests/test_feed_store.py] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| uv | 0.11.2 installed | Python environment and test command runner | Use through `safe-run -- uv run ...` so Python 3.13 and locked dependencies are selected. [VERIFIED: uv --version; uv python list --only-installed; AGENTS.md] |
| safe-run | Available at `/home/shuojing/.local/bin/safe-run` | Host-stability wrapper | Use for all agent-run tests/builds/install-like commands. [VERIFIED: command -v safe-run; prompt AGENTS.md instructions] |
| Ruff | 0.15.12 locked | Formatting/linting | Use through project tasks if code changes are planned; not required for this docs-only artifact. [VERIFIED: uv.lock; .agents/instructions.md] |
| ty | 0.0.42 locked | Python type checking | Use through project lint tasks when code changes touch typed backend contracts. [VERIFIED: uv.lock; .agents/instructions.md] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| New policy dependency | Pydantic/attrs/custom runtime objects | Do not add a dependency; stdlib dataclasses/enums match current backend patterns and keep Phase 1 minimal. [VERIFIED: .planning/PROJECT.md; backend/pipeline/ingestion/models.py] |
| New DB lifecycle status | `suppressed`, `held`, or `retry_suppressed` status | Out of scope for v1; `failing` keeps scheduler compatibility without a migration. [VERIFIED: .planning/PROJECT.md; .planning/ROADMAP.md] |
| Persistent policy audit table | Dedicated DB event/audit table | Deferred to v2; v1 persists only current-schema feed state and later phases add logs. [VERIFIED: .planning/REQUIREMENTS.md; .planning/PROJECT.md] |

**Installation:**

```bash
# No new packages are required for Phase 1.
```

**Version verification:** Python package versions were verified from `uv.lock`, and available runtimes/tools were verified from local commands. [VERIFIED: uv.lock; python3 --version; uv --version; uv python list --only-installed]

## Architecture Patterns

### System Architecture Diagram

```text
Collector signal / runtime pipeline failure
        |
        v
Typed FeedFailure with FailurePolicyEvidence
or runtime-synthesized UNKNOWN telemetry-gap evidence
        |
        v
failure_policy.classify_failure_policy(status_reason, evidence)
        |
        +--> FailurePolicyDecision: quarantine_feed / increment budget
        |         |
        |         v
        |   CollectorRuntime calls FeedStore.report_feed_failure(...)
        |   (Phase 2 execution path; existing budgeted SQL remains sole incrementer)
        |
        +--> FailurePolicyDecision: suppress_retry / hold_for_replay / telemetry_gap
                  |
                  v
            FeedStore.release_non_budgeted_failure(...)
            status='failing', failure_count=0, retry_after, status_reason,
            worker_id=NULL, no quarantine_reason write
                  |
                  v
            Recovery claim path leases failing rows after retry_after
```

This flow preserves the existing layer split: collectors classify source evidence, the policy module is pure, runtime executes decisions, and storage persists state transitions. [VERIFIED: .planning/codebase/ARCHITECTURE.md; 01-CONTEXT.md; backend/pipeline/storage/feed_queries.py]

### Recommended Project Structure

```text
backend/pipeline/ingestion/
├── failure_policy.py              # policy vocabulary, decision model, pure predicates
├── models.py                      # capture boundary types; imports policy evidence
├── collectors/
│   ├── failure_classification.py  # collector helpers building typed failures
│   └── README.md                  # collector authoring guidance
└── tests/
    ├── test_failure_policy.py     # new focused policy contract tests
    └── test_collector_runtime.py  # runtime behavior tests in later phases

backend/pipeline/storage/
├── feed_store.py                  # FeedStatusReason and FeedStore methods
├── feed_queries.py                # fenced SQL constants
└── tests/test_feed_store.py       # SQL/method state contract tests
```

This structure follows the locked ownership boundary and the existing SQL-constant/thin-store pattern. [VERIFIED: 01-CONTEXT.md; backend/pipeline/storage/feed_store.py; backend/pipeline/storage/feed_queries.py]

### Pattern 1: Evidence Facts and Decision Verdicts Stay Separate

**What:** `FailurePolicyEvidence` contains only facts, while `FailurePolicyDecision` carries `policy_intent`, `executed_action`, and budget/quarantine booleans. [VERIFIED: 01-CONTEXT.md]

**When to use:** Use this for every typed `FeedFailure` and runtime fallback decision so the runtime does not parse raw reason text. [VERIFIED: 01-CONTEXT.md; .planning/REQUIREMENTS.md]

**Example:**

```python
# Source: 01-CONTEXT.md D-02/D-04 plus existing StrEnum/dataclass pattern.
@dataclasses.dataclass(frozen=True)
class FailurePolicyEvidence:
    owner_scope: OwnerScope
    failure_scope: FailureScope
    endpoint_kind: EndpointKind
    pipeline_stage: PipelineStage | None = None


@dataclasses.dataclass(frozen=True)
class FailurePolicyDecision:
    evidence: FailurePolicyEvidence
    policy_intent: PolicyIntent
    executed_action: ExecutedAction
    feed_budget_eligible: bool
    quarantine_feed: bool
```

### Pattern 2: Pure Predicate Helpers Wrap Decision Semantics

**What:** Implement `is_feed_quarantine`, `is_feed_budget_eligible`, `is_pipeline_hold`, and `is_source_class_breaker` over `FailurePolicyDecision`, not over string prefixes. [VERIFIED: 01-CONTEXT.md]

**When to use:** Use these helpers at runtime/store call sites so route selection stays readable and testable. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/collector_runtime.py]

**Example:**

```python
# Source: 01-CONTEXT.md D-02/D-20.
def is_feed_budget_eligible(decision: FailurePolicyDecision) -> bool:
    return decision.feed_budget_eligible


def is_pipeline_hold(decision: FailurePolicyDecision) -> bool:
    return (
        decision.evidence.owner_scope is OwnerScope.PIPELINE
        and decision.policy_intent is PolicyIntent.HOLD_FOR_REPLAY
    )
```

### Pattern 3: Storage Uses Fenced SQL Constants plus Thin Methods

**What:** Add storage behavior as a SQL constant in `feed_queries.py` and a thin async method in `FeedStore`. [VERIFIED: backend/pipeline/storage/feed_queries.py; backend/pipeline/storage/feed_store.py]

**When to use:** Use this for `release_non_budgeted_failure(...)` so lease ownership and DB state transition are atomic. [VERIFIED: backend/pipeline/storage/feed_queries.py; 01-CONTEXT.md]

**Example:**

```sql
-- Source: backend/pipeline/storage/feed_queries.py pattern.
UPDATE feeds
SET status = 'failing'::feed_status,
    failure_count = 0,
    worker_id = NULL,
    retry_after = $4,
    unclaimed_since = NOW(),
    status_reason = $5,
    status_reason_updated_at = NOW()
WHERE id = $1 AND worker_id = $2
  AND fencing_token = $3
  AND status = 'active'::feed_status
RETURNING status::text, failure_count, retry_after
```

### Anti-Patterns to Avoid

- **Putting policy verdict fields on `FailurePolicyEvidence`:** D-04 explicitly forbids this; put verdicts on `FailurePolicyDecision`. [VERIFIED: 01-CONTEXT.md]
- **Routing from `status_reason` prefixes:** Prefixes are operator taxonomy only; use `FailurePolicyDecision`. [VERIFIED: 01-CONTEXT.md]
- **Parsing `quarantine_reason`:** It is raw forensic text and migration comments say no CHECK constraint is enforced. [VERIFIED: terraform/modules/alloydb/sql/ingestion/020_quarantine_reason.sql; 01-CONTEXT.md]
- **Adding a new lifecycle status:** `feed_status` is a PostgreSQL enum with existing values, and v1 reuses `failing`. [VERIFIED: terraform/modules/alloydb/sql/ingestion/001_feed_status.sql; .planning/PROJECT.md]
- **Changing progress/observation clearing while adding suppressed retry:** `UPDATE_PROGRESS_SQL` and `RECORD_SOURCE_OBSERVATION_SQL` already clear stale failure state. [VERIFIED: backend/pipeline/storage/feed_queries.py]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Policy classification | Runtime string parser over `reason`, `quarantine_reason`, or `source_`/`system_`/`pipeline_` prefixes | `failure_policy.classify_failure_policy(status_reason, evidence)` plus typed predicates | Raw strings are forensic/operator detail, not routing authority. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/collectors/README.md] |
| Non-budgeted retry persistence | New ad hoc storage update inside runtime | `FeedStore.release_non_budgeted_failure(...)` | Existing code centralizes feed lifecycle mutations in `FeedStore`/SQL constants. [VERIFIED: .planning/codebase/ARCHITECTURE.md; backend/pipeline/storage/feed_store.py] |
| Quarantine budget updates | Any new method that increments `failure_count` | Existing `report_feed_failure(...)` only | Phase 1 requires `report_feed_failure(...)` to remain the only budget increment path. [VERIFIED: .planning/REQUIREMENTS.md; backend/pipeline/storage/feed_queries.py] |
| Durable replay after post-bookmark publish failure | In-memory retry/outbox substitute | Defer durable outbox/hold-replay to v2 | v1 must record the gap and not pretend future ingestion replays the bookmarked message. [VERIFIED: .planning/PROJECT.md; .planning/REQUIREMENTS.md] |
| Source-class breaker state | Per-feed breaker flags or hidden global state | Defer breaker persistence to v2 | v1 only needs policy intent and non-budgeted suppression. [VERIFIED: .planning/REQUIREMENTS.md; 01-CONTEXT.md] |

**Key insight:** The hard part is preserving ownership boundaries, not inventing algorithms; every side effect already has an owner in the codebase. [VERIFIED: .planning/codebase/ARCHITECTURE.md; 01-CONTEXT.md]

## Current Worktree Observations

- The worktree has modified files in `backend/pipeline/common/gcp_helper.py`, `backend/pipeline/ingestion/collector_runtime.py`, collector docs/helpers, ingestion models/tests, and storage files/tests. [VERIFIED: git status --short]
- `failure_policy.py` is absent, while policy enums/classes currently appear in `models.py`. [VERIFIED: file existence check; backend/pipeline/ingestion/models.py]
- Current `FailurePolicyEvidence` includes `policy_intent` and `executed_action`, which conflicts with D-04. [VERIFIED: backend/pipeline/ingestion/models.py; 01-CONTEXT.md]
- Current `FeedFailure` accepts optional policy evidence and tests assert optional evidence, which conflicts with D-05. [VERIFIED: backend/pipeline/ingestion/models.py; backend/pipeline/ingestion/tests/test_collector_runtime.py; 01-CONTEXT.md]
- Current storage already contains `FeedStatusReason.PIPELINE_PUBLISH_AFTER_BOOKMARK_FAILED`, `RELEASE_NON_BUDGETED_FAILURE_SQL`, `FeedStore.release_non_budgeted_failure(...)`, and tests for the non-budgeted SQL/method. [VERIFIED: backend/pipeline/storage/feed_store.py; backend/pipeline/storage/feed_queries.py; backend/pipeline/storage/tests/test_feed_store.py]
- Current runtime tests include Phase 2/3-like behavior for non-budgeted publish gaps and telemetry, but Phase 1 roadmap scopes runtime routing and telemetry to later phases. [VERIFIED: backend/pipeline/ingestion/tests/test_collector_runtime.py; .planning/ROADMAP.md]

## Common Pitfalls

### Pitfall 1: Satisfying POL-02 by Violating D-04

**What goes wrong:** The implementation puts `policy_intent` and `executed_action` directly on `FailurePolicyEvidence`. [VERIFIED: backend/pipeline/ingestion/models.py; 01-CONTEXT.md]

**Why it happens:** Requirement POL-02 says "policy evidence model includes" those fields, but the later locked decision D-04 refines ownership and moves verdict fields to `FailurePolicyDecision`. [VERIFIED: .planning/REQUIREMENTS.md; 01-CONTEXT.md]

**How to avoid:** Treat the policy contract as evidence plus decision; tests should assert evidence has only facts and decision has verdict/action fields. [VERIFIED: 01-CONTEXT.md]

**Warning signs:** `FailurePolicyEvidence(policy_intent=..., executed_action=...)` appears in source or tests. [VERIFIED: backend/pipeline/ingestion/models.py]

### Pitfall 2: Keeping Typed `FeedFailure` Optional

**What goes wrong:** Known/classified collector failures can be created without policy evidence and silently route to telemetry-gap behavior. [VERIFIED: backend/pipeline/ingestion/models.py; backend/pipeline/ingestion/tests/test_collector_runtime.py]

**Why it happens:** Existing dirty code still accepts `policy_evidence=None`; D-05 now requires strict evidence. [VERIFIED: backend/pipeline/ingestion/models.py; 01-CONTEXT.md]

**How to avoid:** Make `policy_evidence` a required keyword-only constructor argument for `FeedFailure`; reserve runtime-synthesized UNKNOWN evidence for untyped exceptions only. [VERIFIED: 01-CONTEXT.md]

**Warning signs:** Tests named around "optional policy evidence" or direct `FeedFailure(status_reason, reason)` construction remain after Phase 1. [VERIFIED: backend/pipeline/ingestion/tests/test_collector_runtime.py]

### Pitfall 3: Accidentally Writing `quarantine_reason` in the Non-Budgeted Path

**What goes wrong:** Suppressed retry overwrites or creates quarantine forensic data even though no quarantine episode exists. [VERIFIED: 01-CONTEXT.md]

**Why it happens:** `report_feed_failure(...)` uses raw `reason` for `quarantine_reason` on quarantine transitions, and a copied SQL path could preserve that behavior. [VERIFIED: backend/pipeline/storage/feed_store.py; backend/pipeline/storage/feed_queries.py]

**How to avoid:** Keep `quarantine_reason` out of `RELEASE_NON_BUDGETED_FAILURE_SQL` and assert absence in tests. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py]

**Warning signs:** `quarantine_reason` appears in the non-budgeted SQL constant. [VERIFIED: backend/pipeline/storage/feed_queries.py]

### Pitfall 4: Breaking Recovery Semantics While Resetting Failure Count

**What goes wrong:** Successful audio progress or `SourceObservation` no longer clears stale `failure_count`/`status_reason`, or non-budgeted failing rows are not claimable after `retry_after`. [VERIFIED: backend/pipeline/storage/feed_queries.py; .planning/ROADMAP.md]

**Why it happens:** Feed lifecycle columns are updated by a small number of atomic SQL paths, and changing one path can affect scheduler recovery. [VERIFIED: .planning/codebase/CONCERNS.md; backend/pipeline/storage/feed_queries.py]

**How to avoid:** Preserve `UPDATE_PROGRESS_SQL`, `RECORD_SOURCE_OBSERVATION_SQL`, and the recovery claim predicate for `status='failing' AND retry_after <= NOW()`. [VERIFIED: backend/pipeline/storage/feed_queries.py]

**Warning signs:** Tests stop checking `status_reason = NULL`, `failure_count = 0`, or retryable failing claim behavior. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py]

### Pitfall 5: Running Broad Tests by Accident

**What goes wrong:** `pyproject.toml` has `addopts = "-n auto"`, and unscoped pytest commands can fan out across resource-heavy suites. [VERIFIED: pyproject.toml; AGENTS.md]

**Why it happens:** The repository has Docker/testcontainers/E2E lanes and xdist configuration. [VERIFIED: .planning/codebase/TESTING.md; pyproject.toml]

**How to avoid:** Use targeted commands with `safe-run --` and `-n 0` when running focused local tests. [VERIFIED: AGENTS.md; .agents/instructions.md; pyproject.toml]

**Warning signs:** Commands such as unscoped `uv run pytest`, `uv run pytest integration_tests/`, or Docker/E2E tasks appear in a Phase 1 plan. [VERIFIED: AGENTS.md; .agents/instructions.md]

## Code Examples

Verified patterns from local sources:

### Strict Typed Failure Boundary

```python
# Source: 01-CONTEXT.md D-05/D-06 and existing FeedFailure normalization pattern.
class FeedFailure(Exception):
    def __init__(
        self,
        status_reason: FeedStatusReason | str,
        reason: str,
        *,
        policy_evidence: FailurePolicyEvidence,
    ) -> None:
        self.status_reason = FeedStatusReason(status_reason)
        self.reason = _validate_reason(reason)
        self.policy_evidence = policy_evidence
        Exception.__init__(self, self.reason)
```

This keeps typed collector failures evidence-backed while leaving runtime fallback evidence synthesis outside `FeedFailure`. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/models.py]

### Feed-Config Quarantine Evidence

```python
# Source: 01-CONTEXT.md D-04 and existing missing_source_feed_id_failure path.
evidence = FailurePolicyEvidence(
    owner_scope=OwnerScope.FEED,
    failure_scope=FailureScope.FEED,
    endpoint_kind=EndpointKind.FEED_CONFIGURATION,
)
decision = classify_failure_policy(
    FeedStatusReason.SYSTEM_CONFIGURATION_INVALID,
    evidence,
)
```

This pattern lets collector helpers create facts while the policy module derives the quarantine action. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/collectors/failure_classification.py]

### Non-Budgeted Storage Test Shape

```python
# Source: backend/pipeline/storage/tests/test_feed_store.py.
sql = _sql_without_comments(feed_queries.RELEASE_NON_BUDGETED_FAILURE_SQL)
assert "status = 'failing'::feed_status" in sql
assert "failure_count = 0" in sql
assert "retry_after = $4" in sql
assert "status_reason = $5" in sql
assert "quarantine_reason" not in sql
```

This test shape is appropriate for Phase 1 because it validates the SQL contract without requiring a local database stack. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py; AGENTS.md]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `FeedFailure`, `_PipelineFailure`, and unexpected runtime exceptions can all consume the feed failure budget. | Only feed-owned `quarantine_feed` decisions may use `report_feed_failure(...)`; non-budgeted decisions release to retryable `failing` with `failure_count=0`. | Planned v1 roadmap, Phase 1/2 split dated 2026-06-14 to 2026-06-15. [VERIFIED: .planning/ROADMAP.md; 01-CONTEXT.md] | Prevents non-feed-actionable failures from burning quarantine budget. [VERIFIED: .planning/PROJECT.md] |
| Raw `quarantine_reason` can be tempting as policy input. | `quarantine_reason` remains forensic; structured evidence and decisions route policy. | Locked in Phase 1 context dated 2026-06-15. [VERIFIED: 01-CONTEXT.md] | Avoids brittle behavior tied to raw strings. [VERIFIED: .planning/REQUIREMENTS.md] |
| Post-bookmark Pub/Sub publish failure was represented as a feed failure workaround. | V1 records suppressed retry and explicit known data gap, while durable replay remains deferred. | Planned v1 roadmap. [VERIFIED: .planning/PROJECT.md; .planning/REQUIREMENTS.md] | Stops feed quarantine while keeping replay-missing reality visible. [VERIFIED: .planning/PROJECT.md] |

**Deprecated/outdated:**

- Treating `failure_count` as all consecutive failures is outdated for v1; it should mean consecutive feed-budget-eligible failures only. [VERIFIED: 01-CONTEXT.md]
- Treating `pipeline_` status reason prefixes as routing authority is outdated; use `FailurePolicyDecision`. [VERIFIED: 01-CONTEXT.md]
- Storing policy enums/classes in `models.py` is not the locked long-term structure. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/models.py]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|

No `[ASSUMED]` claims were intentionally introduced; all recommendations above are based on local project artifacts, local code inspection, local environment probes, or cited OWASP documentation. [VERIFIED: research command outputs; CITED: https://owasp.org/www-project-application-security-verification-standard/]

## Open Questions (RESOLVED)

1. **RESOLVED: How should the planner handle the dirty prior implementation?**
   - Resolution: reconcile the dirty implementation in place. Executors should read current diffs, preserve useful edits that align with D-IDs, and refactor mismatched pieces rather than discarding the whole attempt or duplicating constants/classes. [VERIFIED: git status --short; backend/pipeline/ingestion/models.py; 01-CONTEXT.md]
   - Planning impact: Phase 1 plans explicitly call out reconciliation tasks for policy ownership, strict evidence, collector call sites, and storage primitives. [VERIFIED: 01-CONTEXT.md]

2. **RESOLVED: Does POL-02 require wording cleanup later?**
   - Resolution: interpret POL-02 as the overall policy contract per D-04. `owner_scope`, `failure_scope`, `endpoint_kind`, and optional `pipeline_stage` live on `FailurePolicyEvidence`; `policy_intent` and `executed_action` live on `FailurePolicyDecision`. [VERIFIED: .planning/REQUIREMENTS.md; 01-CONTEXT.md]
   - Planning impact: implementation must honor D-04 even though the requirement uses the older phrase "policy evidence model"; plans should trace POL-02 through both evidence and decision artifacts. [VERIFIED: 01-CONTEXT.md]

3. **RESOLVED: Should runtime telemetry tests remain in Phase 1 plans?**
   - Resolution: keep Phase 1 runtime telemetry coverage only where needed to reconcile existing dirty edits and maintain compilation around the new policy contract. End-to-end runtime routing and telemetry behavior remain Phase 2/3 scope. [VERIFIED: backend/pipeline/ingestion/tests/test_collector_runtime.py; .planning/ROADMAP.md]
   - Planning impact: Phase 1 verification should focus on policy/storage primitives, strict `FeedFailure` evidence, and local import/call-site reconciliation; it should not add broad runtime routing or telemetry acceptance gates beyond what existing edits require. [VERIFIED: .planning/ROADMAP.md; 01-CONTEXT.md]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| safe-run | Host-stable test/build wrapper | Yes | `/home/shuojing/.local/bin/safe-run` | None needed. [VERIFIED: command -v safe-run] |
| uv | Python environment/test runner | Yes | 0.11.2 | Direct Python 3.13 path exists, but use uv for lockfile consistency. [VERIFIED: uv --version; uv python list --only-installed] |
| Python 3.13 | Project runtime | Yes via uv-managed CPython | 3.13.12 | `python3` is 3.12.13 and should not be used directly for project tests. [VERIFIED: uv python list --only-installed; python3 --version; pyproject.toml] |
| pytest / pytest-asyncio | Focused validation | Yes via lockfile | pytest 9.0.3 / pytest-asyncio 1.3.0 | Do not run unscoped tests; use targeted safe-run commands. [VERIFIED: uv.lock; pyproject.toml; AGENTS.md] |
| PostgreSQL/AlloyDB local stack | Optional integration validation | Not probed | — | Phase 1 can use SQL-string and mocked-pool tests; DB integration requires explicit approval. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py; AGENTS.md] |
| gsd-sdk | Planning workflow metadata | Yes | 1.39.0 | None needed. [VERIFIED: gsd-sdk --version] |

**Missing dependencies with no fallback:**

- None for Phase 1 planning and focused unit/storage validation. [VERIFIED: environment probes; .planning/codebase/TESTING.md]

**Missing dependencies with fallback:**

- Direct `python3` is version 3.12.13, but uv-managed Python 3.13.12 is installed and should be selected through uv. [VERIFIED: python3 --version; uv python list --only-installed; pyproject.toml]

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 9.0.3 plus pytest-asyncio 1.3.0. [VERIFIED: uv.lock] |
| Config file | `pyproject.toml`, with `asyncio_mode = "auto"` and `addopts = "-n auto"`. [VERIFIED: pyproject.toml] |
| Quick run command | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py -q -n 0` [VERIFIED: AGENTS.md; pyproject.toml; .planning/codebase/TESTING.md] |
| Full suite command | Do not run broad local suites by default; for this phase use targeted backend files with `-n 0`, and leave full E2E/resource validation to CI unless approved. [VERIFIED: AGENTS.md; .agents/instructions.md] |

### Phase Requirements -> Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| POL-01 | Runtime-facing policy can be represented without raw reason parsing. | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0` | No; create in Wave 0. [VERIFIED: rg --files] |
| POL-02 | Evidence facts plus decision verdict/action cover policy contract fields. | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0` | No; create in Wave 0. [VERIFIED: rg --files; 01-CONTEXT.md] |
| POL-03 | Pipeline-owned evidence can include pipeline stage detail. | unit | `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py -q -n 0` | No; create in Wave 0. [VERIFIED: rg --files; backend/pipeline/ingestion/models.py] |
| STORE-01 | Non-budgeted storage releases lease and writes `failing`. | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` | Yes. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py] |
| STORE-02 | Non-budgeted storage writes `failure_count=0`. | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql -q -n 0` | Yes. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py] |
| STORE-03 | Non-budgeted storage writes `retry_after` and `status_reason`. | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReleaseNonBudgetedFailure -q -n 0` | Yes. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py] |
| STORE-04 | Non-budgeted storage never writes `quarantine_reason`. | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql -q -n 0` | Yes. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py] |
| STORE-05 | `report_feed_failure(...)` is the only budget increment path. | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestNonBudgetedFailureSql backend/pipeline/storage/tests/test_feed_store.py::TestReportFailureSqlStatusReason -q -n 0` | Yes, but add explicit increment-path assertion if absent after reconciliation. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py; backend/pipeline/storage/feed_queries.py] |
| STORE-06 | Progress and `SourceObservation` clear stale failure state. | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestStatusReasonClearSql backend/pipeline/storage/tests/test_feed_store.py::TestRecordSourceObservation -q -n 0` | Yes. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py] |
| STAT-01 | `FeedStatusReason` includes `pipeline_publish_after_bookmark_failed`. | unit | `safe-run -- uv run python -m pytest backend/pipeline/storage/tests/test_feed_store.py::TestFeedStatusReason -q -n 0` | Yes. [VERIFIED: backend/pipeline/storage/tests/test_feed_store.py] |

### Sampling Rate

- **Per task commit:** run the narrowest file/class command that covers the edited behavior, with `safe-run --` and `-n 0`. [VERIFIED: AGENTS.md; pyproject.toml]
- **Per wave merge:** run `safe-run -- uv run python -m pytest backend/pipeline/ingestion/tests/test_failure_policy.py backend/pipeline/storage/tests/test_feed_store.py -q -n 0`. [VERIFIED: .planning/codebase/TESTING.md]
- **Phase gate:** targeted Phase 1 unit/storage tests green, plus `git diff --check`; do not run broad E2E/resource suites without approval. [VERIFIED: AGENTS.md; .agents/instructions.md]

### Wave 0 Gaps

- [ ] `backend/pipeline/ingestion/failure_policy.py` does not exist and must be created as the policy owner. [VERIFIED: file existence check; 01-CONTEXT.md]
- [ ] `backend/pipeline/ingestion/tests/test_failure_policy.py` does not exist and should cover evidence/decision split, strict classifier behavior, and predicates. [VERIFIED: rg --files; 01-CONTEXT.md]
- [ ] Existing `FeedFailure` tests currently assert optional policy evidence; update them to strict evidence or move policy-contract tests to `test_failure_policy.py`. [VERIFIED: backend/pipeline/ingestion/tests/test_collector_runtime.py; 01-CONTEXT.md]

## Security Domain

The OWASP ASVS project describes ASVS as a basis for testing web application technical security controls and secure development requirements; the latest stable version shown by OWASP is 5.0.0. [CITED: https://owasp.org/www-project-application-security-verification-standard/] Phase 1 does not add authentication, sessions, external API routes, cryptography, or new persistence tables. [VERIFIED: .planning/ROADMAP.md; .planning/REQUIREMENTS.md]

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | No | No auth surface changes in Phase 1. [VERIFIED: .planning/ROADMAP.md; .planning/REQUIREMENTS.md] |
| V3 Session Management | No | No session/cookie/token surface changes in Phase 1. [VERIFIED: .planning/ROADMAP.md; .planning/REQUIREMENTS.md] |
| V4 Access Control | Yes, internal worker ownership | Keep `worker_id`, `fencing_token`, and active-status guards on storage transitions. [VERIFIED: backend/pipeline/storage/feed_queries.py] |
| V5 Validation, Sanitization and Encoding | Yes | Validate status reasons via `FeedStatusReason`; use parameterized SQL placeholders for storage writes. [VERIFIED: backend/pipeline/storage/feed_store.py; backend/pipeline/storage/feed_queries.py] |
| V6 Stored Cryptography | No | No crypto/key material changes in Phase 1. [VERIFIED: .planning/REQUIREMENTS.md] |
| V7 Error Handling and Logging | Yes | Keep raw `reason` bounded/safe and do not log secrets or high-cardinality payloads. [VERIFIED: backend/pipeline/ingestion/collectors/README.md; .planning/codebase/CONCERNS.md] |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Worker without current lease mutates feed state | Tampering | Fenced `WHERE id = $1 AND worker_id = $2 AND fencing_token = $3 AND status = 'active'::feed_status`. [VERIFIED: backend/pipeline/storage/feed_queries.py] |
| Raw reason text drives policy or leaks sensitive values | Information Disclosure / Tampering | Treat raw `reason`/`quarantine_reason` as forensic only; use structured evidence for routing and keep reason strings bounded/safe. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/collectors/README.md] |
| SQL injection in feed lifecycle writes | Tampering | Use SQL constants with asyncpg placeholders; only enum-derived values should be passed for `status_reason`. [VERIFIED: backend/pipeline/storage/feed_queries.py; backend/pipeline/storage/feed_store.py] |
| Non-actionable incidents poison feed quarantine budget | Denial of Service | Keep `report_feed_failure(...)` as sole increment path and reset non-budgeted failures to `failure_count=0`. [VERIFIED: .planning/REQUIREMENTS.md; backend/pipeline/storage/feed_queries.py] |

## Sources

### Primary (HIGH confidence)

- `.planning/phases/01-policy-and-storage-foundation/01-CONTEXT.md` - locked Phase 1 decisions and deferred scope.
- `.planning/REQUIREMENTS.md` - POL/STORE/STAT requirement definitions and v2 exclusions.
- `.planning/PROJECT.md` - v1 constraints, core value, and no-migration/no-new-status decisions.
- `.planning/ROADMAP.md` - Phase split, Phase 1 success criteria, and runtime/telemetry deferrals.
- `.planning/codebase/ARCHITECTURE.md` - collector/runtime/storage ownership map.
- `.planning/codebase/CONCERNS.md` - known quarantine-budget mismatch and fragile areas.
- `.planning/codebase/STACK.md` and `.planning/codebase/TESTING.md` - runtime stack and validation guardrails.
- `AGENTS.md` and `.agents/instructions.md` - local testing and workflow constraints.
- `backend/pipeline/ingestion/models.py` - current dirty policy enum/evidence/FeedFailure shape.
- `backend/pipeline/ingestion/collectors/failure_classification.py` and `backend/pipeline/ingestion/collectors/README.md` - collector classification patterns.
- `backend/pipeline/ingestion/collector_runtime.py` - current runtime policy/routing dirty-state context.
- `backend/pipeline/storage/feed_store.py` and `backend/pipeline/storage/feed_queries.py` - status enum, storage methods, and SQL contracts.
- `backend/pipeline/storage/tests/test_feed_store.py` and `backend/pipeline/ingestion/tests/test_collector_runtime.py` - existing focused test patterns and dirty-state tests.
- `terraform/modules/alloydb/sql/ingestion/*.sql` - feed status enum, feed table, status reason, quarantine reason, and HOT/recovery index context.
- `pyproject.toml` and `uv.lock` - Python requirement, pytest config, locked package versions.

### Secondary (MEDIUM confidence)

- OWASP ASVS official project page - ASVS purpose and latest stable version. [CITED: https://owasp.org/www-project-application-security-verification-standard/]

### Tertiary (LOW confidence)

- None.

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH - versions and commands were verified from local lockfiles/config and local environment probes. [VERIFIED: uv.lock; pyproject.toml; command outputs]
- Architecture: HIGH - ownership boundaries are locked in CONTEXT.md and match the codebase map. [VERIFIED: 01-CONTEXT.md; .planning/codebase/ARCHITECTURE.md]
- Pitfalls: HIGH - each pitfall is tied to locked decisions or observed current dirty code/tests. [VERIFIED: 01-CONTEXT.md; backend/pipeline/ingestion/models.py; backend/pipeline/storage/feed_queries.py]
- Security: MEDIUM - phase-specific controls are local and high confidence, while ASVS category framing is cited from OWASP rather than deeply mapped to every ASVS 5.0 requirement. [CITED: https://owasp.org/www-project-application-security-verification-standard/]

**Research date:** 2026-06-15
**Valid until:** 2026-06-22, because the worktree is dirty and code observations may change before planning. [VERIFIED: git status --short]
