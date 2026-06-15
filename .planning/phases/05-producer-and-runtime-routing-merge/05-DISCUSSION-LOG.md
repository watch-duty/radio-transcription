# Phase 5: Producer And Runtime Routing Merge - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-15
**Phase:** 5-producer-and-runtime-routing-merge
**Areas discussed:** Runtime policy execution, Producer status splits, Verification boundary

---

## Runtime Policy Execution

| Option | Description | Selected |
|--------|-------------|----------|
| Shared policy branch | `_PipelineFailure` follows the same classify-and-execute branch as collector `FeedFailure`. | ✓ |
| Status-specific runtime branch | Runtime special-cases Pub/Sub/GCS/bookmark statuses directly. | |
| Keep old pipeline path | All `_PipelineFailure` remains non-budgeted. | |

**User's choice:** Defaulted to shared policy branch based on locked Phase 4
policy and Phase 5 roadmap requirements.

**Notes:** `pipeline_publish_after_bookmark_failed` is budgeted in v1.1.
GCS/bookmark `system_pipeline_error` remains non-budgeted. The canonical policy
log should preserve `replay_missing=true` and `data_gap_known=true` for Pub/Sub
post-bookmark failures, but the old special `post_bookmark_publish_failure`
event is not required for v1.1 routing.

---

## Producer Status Splits

| Option | Description | Selected |
|--------|-------------|----------|
| Split only clear current root causes | Move known env/runtime config, credential access, and malformed source payload cases to the new enum values. | ✓ |
| Split every broad collector error | Aggressively replace `system_collector_error` wherever a narrower label might exist. | |
| Keep old producer labels | Runtime policy changes only; producers stay broad for now. | |

**User's choice:** Defaulted to split only clear current root causes, matching
the prior user decision to add as many status enums as needed but not unused
ones.

**Notes:** `system_configuration_invalid` stays feed-row/source-specific.
`system_runtime_configuration_invalid` covers shared env/deploy/runtime config.
`system_credential_access_failed` covers internal credential retrieval/access
failure. `system_source_payload_invalid` covers successful source responses
that violate collector payload contracts. Ambiguous/mixed cases remain broad.

---

## Verification Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Focused backend tests | Update collector/runtime tests and verify store calls plus narrow behavior. | ✓ |
| Broad compatibility sweep | Include OpenAPI/frontend/generated status surfaces in Phase 5. | |
| Full local integration stack | Run broad local E2E/Docker/emulator checks. | |

**User's choice:** Defaulted to focused backend tests based on roadmap scope and
prior local test safety decisions.

**Notes:** Phase 6 owns OpenAPI/frontend/generated/UI compatibility. Runtime
tests should assert `report_feed_failure(...)` versus
`release_non_budgeted_failure(...)` first; telemetry assertions are secondary.

## the agent's Discretion

- Exact helper names and test parametrization.
- Whether to add optional replay/data-gap flags to `_record_feed_failure(...)`
  or use an equivalent minimal canonical policy-log path.
- Whether to update collector README in Phase 5 only if a local comment/doc
  becomes actively misleading for the implementation.

## Deferred Ideas

- Durable replay/outbox.
- Source-class/credential breaker state.
- Persistent structured audit events.
- Phase 6 API/UI/generated compatibility synchronization.
