# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v1.1 — Policy Merge

**Shipped:** 2026-06-15
**Phases:** 3 | **Plans:** 7 | **Commits since branch base:** 106

### What Was Built

- Explicit `status_reason + evidence` failure policy rows with telemetry-gap fallback.
- Split backend status reasons for runtime configuration invalid, credential access failed, and source payload invalid.
- Producer mappings for Calls, Fire Notifications, Icecast, OpenMHz, and shared payload helpers.
- Runtime `_PipelineFailure` routing through the same policy branch as collector `FeedFailure`.
- Thresholded quarantine behavior for `pipeline_publish_after_bookmark_failed`.
- Non-budgeted reset/retry guards for source, ambiguous collector, GCS/bookmark, credential-access, and telemetry-gap cases.
- Backend collector guide and final backend-only verification artifacts.

### What Worked

- The strict policy table made routing decisions local and reviewable.
- Keeping runtime execution separate from pure policy classification avoided a god module.
- Focused backend tests gave fast feedback without requiring broad local integration stacks.
- GSD phase summaries and verification docs made the final audit straightforward.

### What Was Inefficient

- The design evolved after latest-code synchronization, which required revisiting earlier assumptions.
- Phase 6 initially carried API/UI compatibility language that had to be narrowed back to backend-only closeout.
- Milestone archival exposed small GSD state inconsistencies that needed manual correction.

### Patterns Established

- Route failures from structured `status_reason + evidence`, not forensic `quarantine_reason`.
- Treat unmatched combinations as telemetry gaps rather than inheriting quarantine behavior.
- Record deferred API/UI/generated compatibility explicitly when a milestone is backend-only.
- Keep durable replay, breakers, and persistent audit state as separate future milestones.

### Key Lessons

1. A narrow quarantine policy still needs explicit handling for internal code/deploy fixes when retry cannot repair the issue.
2. Post-bookmark publish failures are different from ordinary pipeline errors because the bookmark/publish gap cannot be repaired by retrying the same feed capture.
3. Documentation must be updated in the same milestone as routing changes; stale authoring guidance can reintroduce the bug class.
4. Compatibility surfaces should be either completed or explicitly deferred with requirement IDs, not left implicit.

### Cost Observations

- Model mix: Codex/GPT-5 execution with no external subagent edits.
- Sessions: multi-session milestone with GSD phase planning, execution, and closeout.
- Notable: narrow backend verification kept runtime manageable while still covering the policy-critical paths.

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Sessions | Phases | Key Change |
|-----------|----------|--------|------------|
| v1.1 | multi-session | 3 | Latest-code merge plus strict backend policy routing and backend-only closeout. |

### Cumulative Quality

| Milestone | Tests | Coverage | Zero-Dep Additions |
|-----------|-------|----------|-------------------|
| v1.1 | 305 focused backend tests plus 47 subtests in final slice | Policy/runtime/storage/collector routing | No new runtime dependency required. |

### Top Lessons

1. Keep policy predicates pure and runtime side effects in runtime/store layers.
2. Prefer explicit route rows over owner-scope defaults for safety-critical quarantine behavior.
3. Preserve deferred compatibility as named follow-up requirements.
