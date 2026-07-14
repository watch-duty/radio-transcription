# Lease Failure Result Simplification

## Context

`IngestionLeaseStore.finalize_failure` currently returns two closed enums:
`LeaseOperationDisposition`, which describes whether the exact fenced mutation
was accepted, and `LeaseFailureEffect`, which describes the resulting failure
state. The effect values duplicate information already represented by
`FeedStatus`: `FAILURE_RECORDED` means `FAILING`, `QUARANTINED` means
`QUARANTINED`, and `NONE` means the operation was not applied.

The storage interface should preserve the final failure status needed for
logging, quarantine telemetry, and parity with `FeedStore`, without exposing
unrelated Lease metadata or adding a second vocabulary for lifecycle status.

## Decision

Remove `LeaseFailureEffect` and change the narrow result to:

```python
@dataclasses.dataclass(frozen=True, slots=True)
class LeaseFailureResult:
    disposition: LeaseOperationDisposition
    final_status: feed_store.FeedStatus | None
```

`LeaseOperationDisposition` remains the shared exact-grant outcome vocabulary.
`final_status` reports only the status produced by an applied failure write.

## Result Contract

| Disposition | Final status | Meaning |
| --- | --- | --- |
| `APPLIED` | `FAILING` | Failure persisted and the Lease is retryable. |
| `APPLIED` | `QUARANTINED` | Budgeted failure reached its threshold. |
| Any rejection | `None` | No failure mutation was applied. |

Construction rejects every other combination. In particular, an applied
result cannot omit its final status, and a rejected result cannot claim a final
status.

## Storage Flow

The existing SQL remains narrow. It locks one permanent Lease identity,
rechecks active owner and fencing token, applies the selected failure action,
and returns `final_status` only for the updated row. Current status, owner, and
fence remain private query evidence used to classify a rejected grant.

`IngestionLeaseStore` converts the returned status into the existing
`FeedStatus` enum and returns it unchanged. Structured logging records
`final_status`; it no longer translates status into a duplicate effect enum.

## Alternatives Rejected

1. Keep `LeaseFailureEffect`: preserves the current interface but duplicates
   `FeedStatus` and adds no independent runtime decision.
2. Return only `LeaseOperationDisposition`: smallest interface, but loses the
   distinction needed for quarantine observability and existing `FeedStore`
   parity.
3. Add failure-specific values to `LeaseOperationDisposition`: mixes generic
   grant acceptance with operation-specific lifecycle state and makes the
   shared disposition vocabulary harder to reuse.

## Scope

Update only the failure result type, conversion logic, logging field, focused
unit/integration contracts, Vulture whitelist, and PR description. Do not
change SQL mutations, schema, retry policy, runtime wiring, child Feed writes,
or cutover behavior.

## Verification

- Unit tests cover every valid and invalid result combination.
- Store tests cover applied failing, applied quarantined, and every rejected
  disposition with `final_status=None`.
- PostgreSQL integration tests continue proving the durable statuses and
  exact-grant no-write behavior.
- Ruff, formatting, type checking, Vulture, focused pytest, and the existing
  PostgreSQL 15/16/17 CI matrix must pass.
