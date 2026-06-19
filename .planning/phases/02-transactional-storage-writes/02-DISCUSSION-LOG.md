# Phase 2: Transactional Storage Writes - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-06-19T13:16:12Z
**Phase:** 2-Transactional Storage Writes
**Areas discussed:** Storage method contract, Meaningful update detection, Snapshot granularity

---

## Restart Handling

The user explicitly asked to restart Phase 2 discussion after an earlier
context had already been captured. This log records the restarted decisions.
The new CONTEXT.md supersedes the earlier Phase 2 context in this directory.

---

## Storage Method Contract

| Option | Description | Selected |
|--------|-------------|----------|
| Require `actor_id` on audited methods | `create_feed`, `update_feed`, `deactivate_feed`, `reset_feed`, and `delete_feed` all require `actor_id`. The service layer supplies a temporary service actor until Phase 3 forwards the real human actor. | Yes |
| Make `actor_id` optional in storage | `FeedStore` defaults missing actors internally. Easier rollout, but it can hide missing attribution. | |
| Add separate audited variants | Keep old methods and add methods like `create_feed_with_audit`. Less disruptive, but creates two mutation paths that can drift. | |

**User's choice:** "actor_id is required."
**Notes:** This rules out optional storage defaults and parallel audited method
variants for Phase 2's storage lifecycle mutation paths.

---

## Meaningful Update Detection

| Option | Description | Selected |
|--------|-------------|----------|
| Suppress no-op audit event | Return the feed normally, but do not write `feed.updated` if no allowlisted value changed. | Yes |
| Emit a no-op `feed.updated` event | Shows that someone attempted an update, but adds noise and makes "updated" less precise. | |
| Treat no-op update as not modified | Return a distinct result/falsey value. Cleaner semantics, but more API/service behavior change for Phase 2. | |

**User's choice:** `1`
**Notes:** No-op update suppression keeps audit history meaningful without
forcing an API behavior change.

---

## Snapshot Granularity

| Option | Description | Selected |
|--------|-------------|----------|
| Hybrid snapshots | `feed.created` and `feed.deleted` use full snapshots; update/deactivate/reset use changed allowlisted fields. | |
| Changed fields only for all events | Smallest payloads, but create/delete become less self-contained unless identity is special-cased elsewhere. | |
| Full allowlisted snapshot for all events | Easiest to inspect and maintain; repeats unchanged state in lifecycle events. | Yes |

**User's choice:** `3`
**Notes:** This changes the earlier Phase 2 direction. `feed.updated`,
`feed.deactivated`, and `feed.reset` should now store full allowlisted
before/after snapshots, not only changed fields.

---

## Carried Forward Without Re-Discussion

- `FeedStore` owns audit row creation; service/runtime callers do not insert
  audit rows directly.
- Phase 2 service fallback actor is `service:feeds-service`.
- Do not use `user:null`, `user:`, or other fake nullable user IDs.
- Remove `system:` from the actor vocabulary before real audit rows are
  emitted.
- Use `feed_audit_event_sequences` transactionally for per-feed sequence
  allocation; do not compute the next sequence with `MAX(feed_sequence) + 1`.

## the agent's Discretion

- Exact helper names and SQL layout.
- Exact implementation of the maintained full snapshot allowlist.
- Exact practical test split between mock-level storage tests and available
  database/component coverage.

## Deferred Ideas

- Trusted user actor forwarding from BFF to feeds service belongs to Phase 3.
- Runtime failure/quarantine/recovery and Echo/sync audit events belong to
  Phase 4.
- Retention enforcement and broad verification hardening belong to Phase 5.
