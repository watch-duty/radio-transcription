# Broadcastify Calls SID cutover evidence index

**Template version:** 1.0  
**Execution/change ID:** ____  
**Environment:** production / ____  
**Result:** NOT STARTED / ABORTED / ACCEPTED / ROLLED BACK  
**Operator:** ____  
**Reviewer:** ____  
**Incident commander:** ____  
**Window start UTC:** ____  
**Window end UTC:** ____

This checked-in file is an index, not the raw evidence store. Copy it into the
approved immutable change system for one execution. Store raw logs, complete
cloud responses, environment files, database credentials, signed URLs, and
private provider payloads only in the approved external system. Record an
external object ID plus SHA-256 here; never paste their contents into Git.

## Immutable inputs

| Field | Reviewed value / external object ID | SHA-256 | Result | Reviewer / UTC |
|---|---|---|---|---|
| public commit | ____ | ____ | ____ | ____ |
| deployment commit | ____ | ____ | ____ | ____ |
| rehearsal-to-candidate delta | ____ | ____ | ____ | ____ |
| candidate image digest | ____ | ____ | ____ | ____ |
| public module pin manifest | ____ | ____ | ____ | ____ |
| saved Terraform plan | ____ | ____ | ____ | ____ |
| operation job revision/identity | ____ | ____ | ____ | ____ |
| Google Cloud SDK version | ____ | ____ | ____ | ____ |
| Context7 docs IDs/date | ____ | n/a | ____ | ____ |
| approved change/backup window | ____ | ____ | ____ | ____ |
| non-production rehearsal | `07-NONPROD-REHEARSAL.md` | ____ | ____ | ____ |
| ASVS review 1 | ____ | ____ | ____ | ____ |
| ASVS review 2 | ____ | ____ | ____ | ____ |

The rehearsal-to-candidate row must review the delta from the rehearsal commits
to the exact public/deployment commits above. The rehearsal is a hard gate but
is not exact-candidate proof.

## Membership and dormant data preparation

| Field | Value / external object ID | SHA-256 | Result | Reviewer / UTC |
|---|---|---|---|---|
| projection/schema verification execution ID | ____ | ____ | ____ | ____ |
| sorted 154-Feed/19-SID manifest | ____ | ____ | ____ | ____ |
| manifest digest | ____ | ____ | ____ | ____ |
| first pre-seed execution ID | ____ | ____ | ____ | ____ |
| first inserted count (0–19) | ____ | n/a | ____ | ____ |
| no-op replay execution ID | ____ | ____ | ____ | ____ |
| replay inserted count (must be 0) | ____ | n/a | ____ | ____ |
| dormant Lease postflight | ____ | ____ | ____ | ____ |
| membership-writer freeze owner | ____ | n/a | ____ | ____ |

## Captured original controls

| Control | Original external object/value | Frozen value | Restored value | Result / reviewer / UTC |
|---|---|---|---|---|
| autoscaler existence, mode, canonical policy | ____ | absent | ____ | ____ |
| autohealing health check and delay | ____ | absent | ____ | ____ |
| VM-failure action | ____ | DO_NOTHING | ____ | ____ |
| MIG target size | ____ | ____ | ____ | ____ |
| MIG versions/template/update policy | ____ | ____ | ____ | ____ |
| numeric member inventory | ____ | unchanged | ____ | ____ |
| worker unit enable/Restart state | ____ | disabled/no | ____ | ____ |
| infrastructure workflow freeze | ____ | frozen | ____ | ____ |
| application workflow freeze | ____ | frozen | ____ | ____ |
| membership-writer freeze | ____ | frozen | ____ | ____ |

## Frozen fleet and process proof

| Artifact | External object ID | SHA-256 | Count/result | Reviewer / UTC |
|---|---|---|---|---|
| frozen MIG inventory (name/zone/numeric instance ID/template/action) | ____ | ____ | ____ | ____ |
| pre-stage worker PID/container/image report | ____ | ____ | ____ | ____ |
| staged key-only report | ____ | ____ | ____ | ____ |
| post-stage unchanged-PID report | ____ | ____ | ____ | ____ |
| legacy graceful/force targets | ____ | ____ | ____ | ____ |
| legacy process-proof rows | ____ | ____ | ____ | ____ |
| immediate pre-activation process-proof rows | ____ | ____ | ____ | ____ |
| immediate post-activation process-proof rows | ____ | ____ | ____ | ____ |
| SID rollback process-proof rows | ____ | ____ | ____ | ____ |
| immediate pre-inverse process-proof rows | ____ | ____ | ____ | ____ |

Every process-proof row must include observation UTC, instance name, zone,
expected and live numeric instance ID, worker index, exact unit, `LoadState`,
`ActiveState`, `SubState`, `MainPID`, exact container name/existence across all
Docker states, `currentAction`, and frozen-inventory equality. Expected count is
exactly twice the frozen VM count. An inaccessible, missing, replaced, or
ambiguous slot is FAIL, never absent.

## State ledger

| State | Entry gate result | Mutation/execution ID | Expected bounded output result | Evidence object/hash | Stop/rollback decision | Operator / Reviewer / UTC |
|---|---|---|---|---|---|---|
| `PREPARED_LEGACY` | ____ | ____ | ____ | ____ | ____ | ____ |
| `FROZEN_LEGACY` | ____ | ____ | ____ | ____ | ____ | ____ |
| `NO_AUTHORITY` | ____ | ____ | ____ | ____ | ____ | ____ |
| `SID_DATA_READY` | ____ | ____ | ____ | ____ | ____ | ____ |
| `SID_BOOTSTRAP` | ____ | ____ | ____ | ____ | ____ | ____ |
| `SID_DURABLE` | ____ | ____ | ____ | ____ | ____ | ____ |
| `SID_SOAK` | ____ | ____ | ____ | ____ | ____ | ____ |
| `ACCEPTED` | ____ | ____ | ____ | ____ | ____ | ____ |
| `ROLLBACK_NO_AUTHORITY` | ____ | ____ | ____ | ____ | ____ | ____ |
| `LEGACY_DATA_READY` | ____ | ____ | ____ | ____ | ____ | ____ |
| `LEGACY_RESTORED` | ____ | ____ | ____ | ____ | ____ | ____ |

Never fill a later state to excuse a missing earlier result. Unused states stay
`NOT EXECUTED`, not PASS.

## SQL execution ledger

| Operation | Terminal execution ID | Started/ended UTC | Bounded report | External log object ID / SHA-256 | Result / reviewer |
|---|---|---|---|---|---|
| verify before pre-seed | ____ | ____ | ____ | ____ | ____ |
| pre-seed | ____ | ____ | ____ | ____ | ____ |
| pre-seed no-op replay | ____ | ____ | ____ | ____ | ____ |
| activation | ____ | ____ | ____ | ____ | ____ |
| post-activation verify | ____ | ____ | ____ | ____ | ____ |
| rollback children (only if invoked) | ____ | ____ | ____ | ____ | ____ |
| post-rollback verify (only if invoked) | ____ | ____ | ____ | ____ | ____ |

Record that every invocation used the closed operation name, sentinel argument,
`--wait`, zero job retries, 300-second job timeout, 5-second lock timeout, and
30-second statement timeout. An outcome-unknown activation is an incident and
must not be shown as retried successfully without an intervening exact
execution inspection and read-only verification.

## Bootstrap and soak evidence

| Field | Value / external object ID | SHA-256 | Result | Reviewer / UTC |
|---|---|---|---|---|
| SID same-slot startup records | ____ | ____ | ____ | ____ |
| startup local numeric-ID/image proof | ____ | ____ | ____ | ____ |
| bootstrap exact filter/window/result limit | ____ | ____ | ____ | ____ |
| bootstrap raw-log object ID | ____ | ____ | ____ | ____ |
| bootstrap envelope | ____ | ____ | ____ | ____ |
| bootstrap reducer output | ____ | ____ | 19/19 / ____ | ____ |
| BOOTSTRAP_END | ____ | n/a | ____ | ____ |
| SOAK_START (strictly later) | ____ | n/a | ____ | ____ |
| SOAK_END (`SOAK_START+30m`) | ____ | n/a | ____ | ____ |
| soak exact filter/window/result limit | ____ | ____ | ____ | ____ |
| soak raw-log object ID | ____ | ____ | ____ | ____ |
| soak envelope | ____ | ____ | ____ | ____ |
| soak reducer output | ____ | ____ | ____ | ____ |
| configured/process legacy-authority proof | ____ | ____ | ____ | ____ |

Record the reducer results for exact SID set, logical polls/minute, attempts per
logical poll, rows per summed exact per-response distinct URL, monotonic
`lastPos`, missing-success gaps, cursor lag, six-poll/60-second hazards,
reacquisition/fence progression, and replay truncation. Explicitly mark
window-wide cross-poll URL uniqueness `NOT EVALUATED` and direct legacy wire
selector inspection `NOT EVALUATED`; the accepted authority evidence is scoped
to the managed frozen fleet.

## Decision and exceptions

**Acceptance or rollback decision:** ____  
**Decision UTC:** ____  
**Operator:** ____  
**Reviewer:** ____  
**Incident owner:** ____  
**Open exceptions (owner/deadline):** ____  
**Unmitigated HIGH findings (must be zero):** ____

## Publication assertions

For the checked-in Plan 07-06 design record, retain both exact statements:

`NO PRODUCTION CUTOVER PERFORMED`

`PITR REVIEWED — NOT EXERCISED`

For a future real execution copy, replace the first statement only with the
actual approved result and evidence IDs. Never change the PITR statement unless
a separate live restore and complete external-fence exercise actually occurs.

