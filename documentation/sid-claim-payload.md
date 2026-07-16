# SID Claim Payload Ownership

- Status: Accepted
- Date: 2026-07-16

## Context

The generic grant interface returns a durable grant and domain-specific runner
payload together. Feed claims need a `LeasedFeed` payload. SID claims need only
the admission mode so later SID processing can label replay-floor evidence as
bootstrap or recovery.

`SidClaimPayload` currently wraps `ClaimMode` and carries a process-local HMAC
binding proof. The HMAC input contains a version tag, the complete Lease grant,
the claim mode, and Python object identity. `SidGrantControl.finalize()` checks
the proof before calling storage.

The proof is not a security seam. Its key and verifier live in the same trusted
Python process as every caller. The grant supervisor already stores one claimed
grant and payload together, and authoritative storage mutations are fenced by
the complete `LeaseGrant`.

The proof also does not protect the point where claim mode is consumed. SID
processing uses claim mode before finalization, while proof validation occurs
only during finalization.

## Decision

Use `grant_control.ClaimMode` directly as the SID runner payload:

```python
grant_control.ClaimedGrant[
    ingestion_lease_store.LeaseGrant,
    grant_control.ClaimMode,
]
```

`SidGrantControl.claim()` returns the requested mode with every claimed grant.
The generic supervisor keeps that exact pair in one managed-grant record and
passes the mode unchanged to the SID runner. Later SID processing uses the mode
only to preserve the bootstrap-versus-recovery replay-floor label.

`SidGrantControl.finalize()` accepts and type-checks `ClaimMode` to satisfy the
shared grant-control interface. It does not use runner payload as authority.
Storage authorizes release and failure finalization exclusively through the
complete fenced `LeaseGrant`.

Remove:

- `SidClaimPayload`;
- `_SID_CLAIM_BINDING_VERSION`;
- `_SID_CLAIM_BINDING_KEY`;
- `_sid_claim_binding_proof()`;
- `_issue_claim_payload()`;
- `_validate_claim_payload()`; and
- the unused `dataclasses`, `hmac`, `json`, and `secrets` imports.

## Correctness Model

The remaining correctness protections are the ones that own real authority:

- `ClaimedGrant` keeps the grant and runner payload together at admission.
- The supervisor registers one immutable grant/payload pairing for an exact
  authority generation.
- The supervisor passes the stored pair to the runner and finalizer without
  reconstructing either value.
- `LeaseGrant` identifies source type, SID, owner worker, and fencing token.
- Fenced storage SQL rejects missing, differently owned, stale, or ineligible
  grants.

A `ClaimMode` value cannot authorize or redirect a storage mutation. Pairing a
valid grant with the wrong mode can at most mislabel replay-floor telemetry; it
does not change cursor selection or the fenced storage target.

## Alternatives Rejected

### Keep the HMAC proof

This preserves an assertion against direct in-process misuse but adds process
keys, proof versioning, JSON serialization, private frozen-dataclass mutation,
and dedicated tests. It detects a mismatch after runner work has already used
the payload and does not strengthen database authority.

### Remove only the proof

Keeping `SidClaimPayload(claim_mode)` would minimize migration changes, but the
result would be a shallow one-field module with no behavior. Passing
`ClaimMode` directly provides the same type safety with less interface surface.

### Add grant identity to the payload

Duplicating source type, SID, owner, or fencing token in runner payload creates
two representations of authority that callers must keep synchronized. The
grant already owns those values.

### Remove claim mode

SID ingestion correctness does not require claim mode after admission, but the
mode currently preserves a useful bootstrap-versus-recovery telemetry label.
Removing that distinction is a separate observability decision and is not part
of this change.

## Verification

Contract tests should establish that:

- primary claims return `ClaimMode.PRIMARY` as payload;
- recovery claims return `ClaimMode.RECOVERY` as payload;
- SID finalization rejects non-`ClaimMode` payload values before storage I/O;
- the grant supervisor forwards one admitted grant/mode pair unchanged;
- release and failure finalization remain fenced by the complete Lease grant;
  and
- later SID cursor selection is identical for primary and recovery modes while
  replay-floor evidence retains the corresponding cause label.
