# Phase 02 Plan Review: Transactional Storage Writes

**Reviewed:** 2026-06-19
**Verdict:** Needs revision

## Coverage Summary

The plans structurally cover the required Phase 2 IDs: `AUD-04`, `EVT-01`,
`EVT-02`, `EVT-03`, `EVT-04`, `EVT-05`, `CON-01`, `CON-02`, `CON-03`, and
`CON-04`.

The SDK structure check passed for all four plans: each plan has three tasks,
required task fields, `must_haves`, and a valid linear dependency chain
(`02-01` -> `02-02` -> `02-03` -> `02-04`).

Security gate: each plan includes a credible `<threat_model>` block. The
blocking security gap is that the actor-constraint cleanup is not enough to
guarantee the locked "no `system:` actor prefix accepted" decision on any
database where the Phase 1 migration has already run.

## Findings

### 1. BLOCKER: `system:` actor removal is not guaranteed on already-applied schemas

**Requirements/decisions:** D-16, CON-04, security gate

The locked context says `system:` must be removed before any audit rows are
emitted (`02-CONTEXT.md:86`). Plan 01 only instructs the executor to remove the
`actor_id LIKE 'system:%'` branch from
`terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql`
(`02-01-PLAN.md:116`).

That does not update databases where the constraint already exists. The
migration currently adds `feed_audit_events_actor_id_check` only inside an
`IF NOT EXISTS` guard (`029_feed_audit_events.sql:76`), and the current
constraint accepts `system:%` (`029_feed_audit_events.sql:120`). The Terraform
schema job re-applies SQL files when content changes (`terraform/modules/alloydb/main.tf:103`),
but the existing guarded constraint block will skip the new definition once the
old constraint exists.

Research explicitly called this out as requiring a migration/precheck plan
(`02-RESEARCH.md:437`), but Plan 01 only records it as a risk
(`02-01-PLAN.md:168`) rather than making it an executable task.

**Fix:** Revise Plan 01 to include an explicit schema update path, such as a
safe precheck for existing `actor_id LIKE 'system:%'` rows plus DDL that drops
and recreates `feed_audit_events_actor_id_check`, or a follow-up migration that
does the same. Add tests that prove an already-existing actor constraint cannot
remain stale.

### 2. BLOCKER: Research open questions are unresolved before planning proceeds

**Dimension:** research resolution

`02-RESEARCH.md` still has a plain `## Open Questions` section, not
`## Open Questions (RESOLVED)`, and both listed questions lack `RESOLVED`
markers (`02-RESEARCH.md:622`).

This matters because the first unresolved question is the same schema-state
risk described above: whether `029_feed_audit_events.sql` has already been
applied to shared/live databases with manual audit rows (`02-RESEARCH.md:624`).
The second leaves the `status_reason_detail` storage-return strategy open
(`02-RESEARCH.md:629`), while the plans proceed with an implicit audit-only
snapshot approach.

**Fix:** Resolve the research section before execution. Either revise
`02-RESEARCH.md` to mark both questions resolved with concrete decisions, or
revise the plans to explicitly cover them. The actor-constraint question should
be resolved by the executable migration/precheck task from Finding 1.

### 3. HIGH: Plan 02 Task 1 verification appears to depend on Task 2 implementation

**Dimension:** task sequencing

Plan 02 Task 1 is scoped to extending transaction-capable test infrastructure
(`02-02-PLAN.md:116`), but its behavior and verification require audited
`create_feed`/`update_feed` behavior that Task 2 has not implemented yet
(`02-02-PLAN.md:120`, `02-02-PLAN.md:126`). The actual implementation task for
audited create/update transactions starts afterward (`02-02-PLAN.md:131`).

If the executor requires each task's `<verify>` command to pass before moving
on, Task 1 will fail for the wrong reason and stall execution.

**Fix:** Move the create/update audit behavior tests and the
`TestUpdateFeedAuditing` verification into Task 2, or split Task 1 so its
verification only checks the transaction-capable mock infrastructure. If the
intent is TDD red/green, make the expected failing step explicit in the plan
format so the executor does not treat it as a failed task.

## Passed Checks

- Requirement frontmatter coverage includes every Phase 2 requirement.
- Plans use existing `FeedStore` mutation methods rather than parallel
  `*_with_audit` variants.
- Plans require storage-level `actor_id` and use `service:feeds-service` as the
  Phase 2 service fallback.
- Plans preserve full allowlisted snapshots for create, update, deactivate,
  reset, and delete, including delete-before-hard-delete timing.
- Plans use `feed_audit_event_sequences` and explicitly reject
  `MAX(feed_sequence) + 1`.
- Runtime failure/quarantine/recovery, trusted admin actor forwarding, admin
  timeline, delivery, and retention remain out of scope.
- Nyquist validation is skipped because `.planning/config.json` sets
  `workflow.nyquist_validation` to `false`.

## Structured Issues

```yaml
issues:
  - severity: BLOCKER
    dimension: context_compliance
    requirement_ids: [CON-04]
    decisions: [D-16]
    plan: "02-01"
    task: 1
    description: "Plan removes system: from the migration text but does not update already-existing feed_audit_events_actor_id_check constraints."
    fix_hint: "Add an executable precheck plus drop/recreate constraint migration path, and test stale-constraint prevention."
  - severity: BLOCKER
    dimension: research_resolution
    plan: null
    description: "02-RESEARCH.md has unresolved Open Questions before execution."
    fix_hint: "Resolve the Open Questions section or revise plans to cover both unresolved decisions."
  - severity: HIGH
    dimension: task_sequencing
    plan: "02-02"
    task: 1
    description: "Task 1 verification depends on audited create/update implementation that is planned in Task 2."
    fix_hint: "Move behavior tests/verification to Task 2 or limit Task 1 verification to the transaction mock helper."
```

## Recommendation

Return to the planner. Do not execute Phase 2 until the actor-constraint
migration path and research resolution are explicit in the plans.
