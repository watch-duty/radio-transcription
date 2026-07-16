# Ingestion Failure Persistence Ownership

- Status: Accepted
- Date: 2026-07-16

## Context

The generic ingestion runtime supervises both Feed grants and SID grants. A
runner classifies a terminal failure, shared policy chooses whether that failure
consumes the durable quarantine budget, and a domain-specific grant control
persists the result through its store.

The failure contract must preserve existing `bcfy_calls` behavior:

- only configuration and runtime-configuration failures consume the durable
  failure budget;
- all other failures retry without consuming that budget and cannot
  quarantine;
- the canonical status reason and diagnostic detail remain visible in durable
  feed state; and
- audited mutations identify the runtime service that performed them.

These values have different owners. Treating all of them as fields of a
budgeted or non-budgeted decision conflates failure evidence, policy treatment,
and persistence context.

## Decision

### Failure evidence

`status_reason` and `reason` travel together as failure evidence:

- `RunFailed` carries them across the runner-to-supervisor seam.
- `FailurePersistencePlan` carries them across the supervisor-to-grant-control
  seam alongside the selected treatment.

`status_reason` is the canonical classification. It selects budget treatment
and is persisted as `status_reason`. `reason` is optional diagnostic detail. It
does not select treatment and is persisted as `status_reason_detail`.

The persistence plan keeps these fields flat. A separate `FailureEvidence`
wrapper would add another type and nested access without eliminating the
existing collector failure models. The two seams intentionally have standalone
values rather than coupling persistence planning to the `RunFailed` outcome
type.

### Failure treatment

The two treatment values contain only data that changes persistence behavior:

- `ConsumeFailureBudget` contains the failure threshold and backoff bounds.
- `RetryWithoutBudget` contains the earliest recovery time.

Neither treatment contains `status_reason`, `reason`, or `actor_id`. Shared
policy selects exactly one treatment from `status_reason` before finalization.
Grant controls execute that selection without reclassifying the failure.

### Audit identity

`actor_id` is process-level persistence context. It is resolved at runtime
composition and injected once into each concrete `FeedGrantControl` or
`SidGrantControl`. The control supplies that identity to every audited store
mutation.

`actor_id` is not part of `RunFailed`, `FailurePersistencePlan`, or a treatment.
This prevents collectors and runners from selecting an audit identity and
avoids repeating a constant value in every terminal decision. Store methods
continue to accept an explicit actor because the same stores serve runtime and
administrative callers with different identities.

## Data Flow

```text
runner
  -> RunFailed(status_reason, reason)
  -> shared failure policy
  -> FailurePersistencePlan(status_reason, reason, treatment)

FeedGrantControl(actor_id) or SidGrantControl(actor_id)
  -> fenced store finalization(
       treatment,
       status_reason,
       reason,
       actor_id,
     )
```

The storage seam applies the length cap to `reason`. Upstream contracts describe
it as diagnostic detail rather than claiming it is already bounded.

## Invariants

- Only `status_reason` selects budgeted versus non-budgeted treatment.
- Non-budgeted treatment never increments the durable failure count and never
  quarantines.
- Grant controls do not rerun failure policy during finalization.
- The configured control actor, not runner-provided data, identifies every
  audited runtime mutation.
- Both Feed and SID controls preserve the same generic terminal semantics while
  translating to their respective stores.
- Storage remains the sole owner of diagnostic-detail truncation.

## Alternatives Rejected

### Duplicate evidence in each treatment

Putting `status_reason` and `reason` in both budgeted and non-budgeted decision
types duplicates common state and makes new evidence fields require parallel
changes. Putting `actor_id` there also misrepresents process context as a
per-failure policy choice.

### Introduce a shared `FailureEvidence` wrapper

A wrapper could eventually unify `FeedFailure`, `FailureInfo`, `ItemFailure`,
`RunFailed`, and `FailurePersistencePlan`. Doing so only for the grant contract
would instead add translation and nesting. A collector-wide normalization can
be considered separately if those models later need coordinated evolution.

### Resolve the actor inside storage

Ambient actor resolution would hide audit provenance and prevent the same store
interface from cleanly serving administrative and runtime mutations.

### Put the actor in runner context or grant authority

The runner does not own persistence identity, and a grant proves durable
ownership rather than audit identity. Combining either concept with `actor_id`
would weaken both interfaces.

## Verification

Contract tests should establish that:

- the policy exposes only classification and treatment inputs;
- persistence plans contain only evidence and one selected treatment;
- each grant control always forwards its configured actor;
- neither grant control reclassifies a finalized plan; and
- non-budgeted finalization rejects an impossible quarantined result.
