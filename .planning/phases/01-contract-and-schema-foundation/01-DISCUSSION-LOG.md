# Phase 1: Contract and Schema Foundation - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-19
**Phase:** 1-Contract and Schema Foundation
**Areas discussed:** Delete Identity, Actor Shape, Detail Safety, Contract Docs

---

## Delete Identity

| Option | Description | Selected |
|--------|-------------|----------|
| Operator identity only | Preserve common operator-facing identity fields after hard delete. | |
| Full current-state snapshot | Preserve all non-secret feed/feed_properties fields. | |
| Minimal IDs only | Preserve only feed/source identifiers. | |
| Required deletion snapshot | `feed.deleted.before_values` must contain enough allowlisted fields to identify the deleted feed. | ✓ |
| Best-effort deletion snapshot | Include whatever the delete path has available. | |
| Full row snapshot | Include all non-secret fields from the current feed row. | |

**User's choice:** Use `before_values`; do not create extra delete-specific identity fields.
**Notes:** User clarified that the `feeds` row is removed on hard delete. The
final decision is to make `feed.deleted.before_values` self-contained using a
long-term-maintainable allowlist derived from the `feeds` row or subset, while
excluding noisy lease fields by default.

---

## Actor Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Raw user email | Human-readable but PII and rename-sensitive. | |
| Namespaced email | Clear fallback but still PII and mutable. | |
| Google subject | Stable, less PII, available on current `GoogleUser.sub`. | ✓ |
| Issuer + subject | Future multi-IdP safe but verbose for current Google-only auth. | |
| BFF service account | Authenticates transport but loses the causal human admin. | |
| Backend service name | Good for service-owned actions. | ✓ |
| Runtime component | Good for runtime/system events. | ✓ |
| Worker/instance ID | Precise but too granular for canonical actor identity. | |
| Scheduled job | Good for maintenance or migration actions. | ✓ |
| Unknown | Explicit fallback. | ✓ |

**User's choice:** Use only `actor_id`, not separate actor fields.
**Notes:** Final contract is one required namespaced `actor_id`: prefer
`user:google:<sub>` for human admins; use `user-email:<normalized_email>` only
as fallback; use `service:`, `system:`, and `job:` for semantic non-human
actors; reserve `gcp-sa:` and `unknown:unknown` for fallback cases.

---

## Detail Safety

| Option | Description | Selected |
|--------|-------------|----------|
| Scrubbed operator summary | Bounded text with obvious credential/token redaction. | |
| Truncated raw exception | Preserve raw failure text with length cap. | ✓ |
| Typed reason only | Store no free-text detail beyond `status_reason`. | |
| Mostly raw + minimal secret redaction | Preserve raw detail but redact obvious auth/token patterns. | |
| Raw cap only | Mirror current `quarantine_reason`: cap only, no redaction. | ✓ |
| Go back to scrubbed summary | Prefer privacy over raw debugging value. | |

**User's choice:** Raw cap only.
**Notes:** The agent flagged that this conflicts with the earlier security
preference to avoid persisting credential-bearing exception strings. User still
selected raw cap only. Context records the security tradeoff explicitly.

---

## Contract Docs

| Option | Description | Selected |
|--------|-------------|----------|
| Domain contract first, schema second | Define event meaning first, storage details as support. | ✓ |
| Storage schema first | Optimize docs for implementers and table layout. | |
| Two separate docs | Separate domain contract doc from storage design doc. | |

**User's choice:** Domain contract first, schema second.
**Notes:** Phase 1 docs should define Feed Audit Event meaning, action
vocabulary, actor ID vocabulary, before/after semantics, diagnostic detail,
retention, and v1 boundaries. Storage schema details should support the domain
contract rather than replace it.

---

## the agent's Discretion

- Exact field allowlist for `before_values`, as long as it is maintainable and
  derived from the `feeds` row/subset.
- Exact migration and schema mechanics, as long as they preserve the locked
  domain contract.

## Deferred Ideas

None.
