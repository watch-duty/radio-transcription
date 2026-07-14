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

This checked-in file is an index, not the retained evidence store. Copy it into
the approved immutable change system for one execution. Reducer-safe projected
log populations and bounded non-secret cloud responses may live there and are
referenced by object ID plus SHA-256. Never collect or store complete
environment files, credential values, signed URLs, private provider payloads,
private keys, or bearer tokens
as evidence anywhere. Leave secrets in Secret Manager and retain only numeric
secret-version resource IDs, key-only projections, and sanitized hashes.

## Immutable inputs

| Field | Reviewed value / external object ID | SHA-256 | Result | Reviewer / UTC |
|---|---|---|---|---|
| public commit | ____ | ____ | ____ | ____ |
| clean public checkout plus signed SLO/bootstrap/soak file hashes | ____ | ____ | ____ | ____ |
| deployment commit | ____ | ____ | ____ | ____ |
| signed immutable/protected deployment tag resolving to deployment commit | ____ | ____ | ____ | ____ |
| signed input manifest | ____ | ____ | ____ | ____ |
| project ID / numeric project ID | ____ | ____ | ____ | ____ |
| rehearsal-to-candidate delta | ____ | ____ | ____ | ____ |
| candidate image digest | ____ | ____ | ____ | ____ |
| public module pin manifest | ____ | ____ | ____ | ____ |
| saved Terraform plan | ____ | ____ | ____ | ____ |
| full MIG URI / bounded description | ____ | ____ | ____ | ____ |
| full autoscaler URI / bounded description | ____ | ____ | ____ | ____ |
| full operation Job URI / generation / identity | ____ | ____ | ____ | ____ |
| saved Terraform/config proof that schema/operation Jobs share the immutable image digest and numeric password-secret version | ____ | ____ | ____ | ____ |
| operation Job identity/generation and match-only bounded attestation (no endpoint/network/secret values) | ____ | ____ | ____ | ____ |
| SQL bucket URI and four object generations/content hashes | ____ | ____ | ____ | ____ |
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
| per-numeric-VM/slot unit enable/Restart state | ____ | disabled/no | enabled/always | ____ |
| infrastructure workflow freeze | ____ | frozen | ____ | ____ |
| application workflow freeze | ____ | frozen | ____ | ____ |
| membership-writer freeze | ____ | frozen | ____ | ____ |
| exact production authority/maintenance/image tuple | ____ | ____ | ____ | ____ |
| active AlloyDB endpoint presence + SHA-256 only (no plaintext endpoint) | ____ | unchanged | ____ | ____ |
| live replacement-template mode/profile/image/endpoint-hash projection | ____ | ____ | ____ | ____ |
| active-container mode/profile/image/endpoint-hash and PID-continuity projection | ____ | ____ | ____ | ____ |
| saved-plan invocation (`suppress_app_deploy=true`) and unique run/head SHA | ____ | ____ | ____ | ____ |
| Terraform plan hash/public pin/contract projection (binary remains temporary) | ____ | ____ | ____ | ____ |
| before/after application workflow-dispatch run IDs (must be identical) | ____ | ____ | ____ | ____ |

## Frozen fleet and process proof

| Artifact | External object ID | SHA-256 | Count/result | Reviewer / UTC |
|---|---|---|---|---|
| frozen MIG inventory (name/zone/numeric instance ID/template/action) | ____ | ____ | ____ | ____ |
| pre-stage worker PID/container/image report | ____ | ____ | ____ | ____ |
| staged key-only report | ____ | ____ | ____ | ____ |
| post-stage unchanged-PID report | ____ | ____ | ____ | ____ |
| legacy graceful/force targets | ____ | ____ | ____ | ____ |
| per-VM ARMED remote/controller epoch delta (absolute ≤5s) | ____ | ____ | ____ | ____ |
| legacy T+90 host-local slot snapshot and exact survivor set | ____ | ____ | ____ | ____ |
| legacy exact force completion at/before T+100 | ____ | ____ | ____ | ____ |
| legacy T+120-or-earlier final complete slot proof | ____ | ____ | ____ | ____ |
| legacy process-proof rows | ____ | ____ | ____ | ____ |
| immediate pre-activation process-proof rows | ____ | ____ | ____ | ____ |
| immediate post-activation process-proof rows | ____ | ____ | ____ | ____ |
| SID rollback process-proof rows | ____ | ____ | ____ | ____ |
| rollback last durable template mode (`legacy_feed` or `sid_lease`) | ____ | ____ | ____ | ____ |
| immediate pre-inverse process-proof rows | ____ | ____ | ____ | ____ |

Every process-proof row must include observation UTC, instance name, zone,
expected and live numeric instance ID, worker index, exact unit, `LoadState`,
`ActiveState`, `SubState`, `MainPID`, `ControlPID`, unit-cgroup process count,
exact container name/existence across all Docker states, configured image and
image ID, per-slot profile/mode/index counts, host-wide authority-container
count, unknown-authority-container count, ingestion-host-process count,
`currentAction`, and frozen-inventory equality. Expected count is exactly twice
the frozen VM count. An inaccessible, missing, replaced, late, or ambiguous
slot/process/container is FAIL, never absent.

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

Before the ledger, retain the signed manifest hash plus the full Job URI and
exact Job generation. The bounded Job/Execution evidence retains only public
resource identity, configured key names, command hash, and match-only booleans
for service account, immutable image, direct port 5432 endpoint/database,
numeric secret reference, one exact network/subnetwork pair, and GCS mount; it
never repeats those private comparison values. Retain bounded before/after object metadata
and local/remote content SHA-256 for each
of `001_preseed.sql`, `002_activate.sql`, `003_rollback_children.sql`, and
`004_verify.sql`. Any changed generation or byte is FAIL.

| Operation | Terminal execution ID | Started/ended UTC | Bounded report | External log object ID / SHA-256 | Result / reviewer |
|---|---|---|---|---|---|
| verify before pre-seed | ____ | ____ | ____ | ____ | ____ |
| pre-seed | ____ | ____ | ____ | ____ | ____ |
| pre-seed no-op replay | ____ | ____ | ____ | ____ | ____ |
| activation | ____ | ____ | ____ | ____ | ____ |
| post-activation verify | ____ | ____ | ____ | ____ | ____ |
| rollback children (only if invoked) | ____ | ____ | ____ | ____ | ____ |
| post-rollback verify (only if invoked) | ____ | ____ | ____ | ____ | ____ |

Record that every invocation used the closed operation name, exact override
argument array and sentinel argument, `--wait`, task count one, zero job
retries, 300-second job timeout, expected full Job binding, start time on/after
the request-issued invocation T0, 5-second lock timeout, and 30-second statement
timeout. Retain the exact validated short Job/execution IDs used with explicit
project and region, the constructed canonical Job-qualified execution URI, the
v1 `metadata`/`status` identity and terminal projection, and byte-equal source
Job `before`/`after` UID/generation/template projections. Never attribute source
Job UID/generation to an Execution row. Retain only the bounded execution
projection—not a full provider response—plus two byte-identical execution-log
reads after the documented ingestion grace. A terminal-failed or
response-unknown mutation is an incident. Only after its single execution is
safely bound, terminal, and immutable inputs pass does the wrapper attempt one
distinct read-only verification before Logging evidence. Missing/ambiguous
discovery, untrusted identity/attestation, nonterminal state, or changed
immutable inputs must record `UNSAFE_TO_VERIFY`, return nonzero, issue no second
execution, and never resubmit the mutation. Record a Logging collection failure
separately; it must not suppress an eligible safety verification.

## Bootstrap and soak evidence

| Field | Value / external object ID | SHA-256 | Result | Reviewer / UTC |
|---|---|---|---|---|
| signed startup contract (profile digests/domains/pacing/admission) | ____ | ____ | ____ | ____ |
| SID same-slot `startup_pacing_complete` first/second projected reads | ____ | ____ | ____ | ____ |
| SID startup exact slot/runtime identities and stable-read envelope | ____ | ____ | ____ | ____ |
| startup local numeric-ID/image proof | ____ | ____ | ____ | ____ |
| exact SLO-contract payload schema/message validation and message-stripped projection | ____ | ____ | ____ | ____ |
| bootstrap exact filter/window/result limit | ____ | ____ | ____ | ____ |
| bootstrap first/second reducer-safe projected-population object IDs | ____ | ____ | ____ | ____ |
| bootstrap stable count / identity-set hashes | ____ | ____ | ____ | ____ |
| bootstrap receiveTimestamp min/max / completed UTC | ____ | ____ | ____ | ____ |
| bootstrap envelope | ____ | ____ | ____ | ____ |
| bootstrap reducer output | ____ | ____ | 19/19 / ____ | ____ |
| BOOTSTRAP_END | ____ | n/a | ____ | ____ |
| SOAK_START (strictly later) | ____ | n/a | ____ | ____ |
| SOAK_END (`SOAK_START+30m`) | ____ | n/a | ____ | ____ |
| soak exact filter/window/result limit | ____ | ____ | ____ | ____ |
| soak first/second reducer-safe projected-population object IDs | ____ | ____ | ____ | ____ |
| soak stable count / identity-set hashes | ____ | ____ | ____ | ____ |
| soak receiveTimestamp min/max / completed UTC | ____ | ____ | ____ | ____ |
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

For each bootstrap/soak collection, evidence is complete only after the exact
half-open window has been closed for 120 seconds and two non-limit-reaching
canonical reads, 30 seconds apart, have identical counts, identity-set hashes,
and full-result hashes. Retain both projected reads, both identity sets, receive-time
bounds, and collection-completed UTC. This is a bounded operational rule, not
an absolute provider no-late-arrival promise; a later matching entry invalidates
the decision evidence.

## Optional PITR incident appendix

Complete this appendix only for a separately approved PITR incident. Its blank
presence does not imply an exercise.

| Evidence | External object / bounded value | SHA-256 | Operator | Reviewer / UTC |
|---|---|---|---|---|
| incident/change ID and command-version review | ____ | ____ | ____ | ____ |
| Gate 0 status (must be CLEARED by reviewed implementation; currently BLOCKED) | ____ | ____ | ____ | ____ |
| signed complete eight-consumer URI/template/invoker inventory | ____ | ____ | ____ | ____ |
| eight rows `REPROVED` / zero uncatalogued database consumers | ____ | ____ | ____ | ____ |
| source/restored `INTERNAL_AUTOMATION` pg_cron flags/Jobs/run-frontier ledgers | ____ | ____ | ____ | ____ |
| migration `019` scheduling fence (disabled exact Jobs, drained runs, pg_cron off) | ____ | ____ | ____ | ____ |
| immutable numeric MIG worker-secret binding (no password/hash/user data) | ____ | ____ | ____ | ____ |
| collector MIG raw-password Gate 0 blocker cleared by reviewed numeric secret binding | ____ | ____ | ____ | ____ |
| source project/region/cluster/primary/network/PSA | ____ | ____ | ____ | ____ |
| continuous backup, earliest restorable time, target UTC | ____ | ____ | ____ | ____ |
| source/restored backup/deletion-protection/labels/pooling/Query-Insights/IaC parity-delta matrix | ____ | ____ | ____ | ____ |
| complete external process-fence object | ____ | ____ | ____ | ____ |
| restored cluster operation/resource description | ____ | ____ | ____ | ____ |
| restored primary operation/resource description | ____ | ____ | ____ | ____ |
| restored data branch and schema/migration report | ____ | ____ | ____ | ____ |
| Secret Manager secret/version resource IDs (no values) | ____ | ____ | ____ | ____ |
| restored admin surface attestation before any client probe | ____ | ____ | ____ | ____ |
| direct admin 5432 redacted identity/schema hash | ____ | ____ | ____ | ____ |
| pooled worker 6432 redacted identity/schema hash | ____ | ____ | ____ | ____ |
| controlled Job before/after full URI/UID/generation/template hashes (source Job only) | ____ | ____ | ____ | ____ |
| validated short Job/execution IDs, canonical execution URI, v1 metadata/status projection, bounded post-T0 result | ____ | ____ | ____ | ____ |
| signed canonical 004 comparator artifact/hash/exit-zero result | ____ | ____ | ____ | ____ |
| selected branch exact 19/154 and byte-equal signed manifest digest | ____ | ____ | ____ | ____ |
| active/source output and all-eight consumer fan-out | ____ | ____ | ____ | ____ |
| startup/fence/bootstrap/soak/control evidence | ____ | ____ | ____ | ____ |
| source fallback hold owner/deadline | ____ | ____ | ____ | ____ |

Record the restored-data classification as exactly one of `activated rows`,
`dormant pre-seed rows`, or `pre-schema`. Retain bounded before/after Lease and
child reports, but never store credentials or raw environment output here.

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
