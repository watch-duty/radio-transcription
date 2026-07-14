# AlloyDB point-in-time recovery with an external authority fence

**Runbook version:** 1.1  
**Status:** **REVIEWED — NOT EXERCISED; LIVE USE BLOCKED BY GATE 0**  
**Scope:** out-of-place restoration of the production AlloyDB timeline and a
coherent switch of every checked-in database consumer

This document is a disaster-recovery design and operator checklist. It has not
created a restored cluster, switched an endpoint, or started a restored worker.
Its checked-in publication means exactly:

`PITR REVIEWED — NOT EXERCISED`

`NO PRODUCTION CUTOVER PERFORMED`

The current deployment does **not** expose a complete, durable database-consumer
fence. Consequently, a live operator must stop at Gate 0 below. This blocker is
specific to a cross-timeline, database-wide PITR switch. It does not block the
same-database Broadcastify Calls SID authority cutover described in
[`bcfy-calls-sid-cutover.md`](bcfy-calls-sid-cutover.md).

Never copy a password, private key, complete environment, signed URL, access
token, or unbounded provider response into Git, an incident document, a shell
history, a CI log, or the evidence store.

## Safety boundary

PITR creates a new database timeline. A fencing token stored in either database
cannot fence a process connected to the other timeline. A Calls-only VM stop is
therefore necessary but insufficient. Before restore, the external fence must
cover all eight database consumers, every invocation source, every in-flight
execution, and every replacement template.

The source remains fenced throughout restore, endpoint convergence, data
classification, validation, startup, and the approved fallback period.
Restored claims remain disabled until:

1. all eight old-timeline consumer rows are durably suppressed, drained, absent,
   and independently reproved;
2. all eight replacement definitions converge on one selected restored endpoint
   while still fenced;
3. controlled direct and pooled probes identify the restored database; and
4. the selected data branch passes `004_verify.sql` with the exact reviewed
   19-SID/154-Feed manifest digest.

Database rows, Lease heartbeats, logs, tests, and a tabletop are diagnostics;
none proves cross-timeline process absence.

## Gate 0: current implementation blocker

The checked-in deployment graph has one complete stop seam, one partial seam,
and six missing seams. The table below is authoritative for this runbook. A row
marked `BLOCKED` cannot be completed by an improvised `gcloud` command.

| # | Endpoint consumer | Checked-in execution surface | Present control | Gate 0 result |
|---:|---|---|---|---|
| 1 | `audio_segments_api` | Cloud Run v2 service | no no-database maintenance revision, complete invoker fence, or drain proof | **BLOCKED** |
| 2 | `bcfy_calls_sid_operation` | manually invoked Cloud Run v2 Job | no durable execution-disable sentinel; secret reference is `latest` | **BLOCKED** |
| 3 | `collector_mig` | regional MIG, two worker slots per VM | `ingestion_maintenance_mode` plus the exact process-absence procedure in the cutover runbook | **AVAILABLE** |
| 4 | `echo_ingestion` | Eventarc trigger to a min-one Cloud Run v2 service | Eventarc has delete but no pause; no checked-in reversible trigger/service fence or drain proof | **BLOCKED** |
| 5 | `feed_store` | Cloud Run v2 service | no no-database maintenance revision, complete invoker fence, or drain proof | **BLOCKED** |
| 6 | `oldest_feed_publisher` | Cloud Scheduler to Cloud Run v2 service | Scheduler can be paused, but the service remains invocable and may have an in-flight request | **INCOMPLETE — BLOCKED** |
| 7 | `rules_management` | Cloud Run v2 service | no no-database maintenance revision, complete invoker fence, or drain proof | **BLOCKED** |
| 8 | `schema_migration` | Terraform-triggered and manually invocable Cloud Run v2 Job | no durable execution-disable sentinel; secret reference is `latest`; Terraform can invoke it | **BLOCKED** |

Cloud Run `min_instance_count=0` is not a stop control: a request can create an
instance. Removing one known invoker is not a complete fence: another principal,
trigger, URL, tagged revision, or already-running request may remain. Pausing the
Publisher scheduler does not fence direct invocation. Eventarc offers no pause;
deleting a trigger without a reviewed recreation and backlog/drain contract is
not an approved reversible fence. Cloud Run Jobs offer execution cancellation,
but no provider-level “disable this job” state. Cancelling current executions
does not prevent a new execution.

Therefore **all restore, endpoint, SQL, and claim operations are blocked today**.
Do not delete services/jobs/triggers, edit IAM, rotate credentials, or improvise
a maintenance image during an incident to get past this gate.

### Implementation required to clear Gate 0

A future reviewed deployment change must add one coherent
`database_recovery_maintenance_mode` (name may differ, semantics may not) that:

- keeps the existing collector MIG maintenance controls and exact slot-level
  absence proof;
- moves each database-capable Cloud Run service to a pinned, no-database
  maintenance revision, removes every route/tag and invocation grant capable of
  reaching an old database revision, and supplies a bounded drain/absence proof;
- durably pauses the Publisher scheduler and suppresses direct Publisher
  invocation;
- durably removes or disables the Echo Eventarc delivery path, drains prior
  delivery, and keeps the Echo service on a no-database revision;
- makes both Cloud Run Jobs non-executable by default, suppresses Terraform
  auto-execution, cancels and proves absent every nonterminal execution, and
  permits only one reviewed, generation-bound controlled execution;
- pins numeric Secret Manager versions for the recovery window instead of
  resolving `latest`;
- keeps all replacement templates fenced while the endpoint changes; and
- has deterministic tests for all eight rows, including restart/redeploy
  suppression and a fail-closed unknown-resource branch.

The implementation review must produce a deployment commit, an immutable image
digest, exact resource bindings, and a successful non-production rehearsal.
Only then may an incident-specific runbook revision replace the `BLOCKED` cells
with exact reviewed commands. Until that happens, the remaining state machine is
a future execution contract, not authorization to mutate production.

## Durable eight-row fence ledger

The future implementation must materialize one signed ledger row for each entry
above. A prose checklist is not a fence. Every row must contain:

- consumer ID and full provider resource URI;
- replacement-template URI or revision/generation;
- all trigger/invoker resource URIs;
- pre-fence generation, UID, image digest, endpoint SHA-256, and numeric secret
  version resource IDs;
- the exact reviewed suppress operation and its terminal operation ID;
- `SUPPRESSED_AT`, maximum request/task timeout, `DRAIN_DEADLINE`, and
  `ABSENCE_PROVED_AT` in UTC;
- bounded pre/post description hashes and a control-state projection;
- a restart/redeploy-suppression proof; and
- operator, independent reviewer, and signature timestamp.

Each row advances monotonically:

`DISCOVERED -> SUPPRESSED -> DRAINED -> ABSENT -> REPROVED`

All eight rows must be `REPROVED` immediately before restore, immediately before
endpoint apply, immediately before every controlled SQL execution, and
immediately before startup. One missing, stale, replaced, ambiguous, or
inaccessible resource resets the complete ledger to **not proven**.

Provider-specific controls must be explicit:

- **MIG:** use the exact autoscaler, autohealing, repair, boot, restart, numeric
  instance-ID, worker-index, cgroup/container, and T+90/T+100/T+120 procedure in
  the SID cutover runbook.
- **Cloud Scheduler:** the reviewed future implementation may use
  `gcloud scheduler jobs pause "$PUBLISHER_SCHEDULER_URI"
  --project="$PROJECT" --location="$REGION"`, then require `state=PAUSED`.
  This is only one part of row 6; it does not stop the Cloud Run service.
- **Eventarc:** the current SDK has deletion, not pause. A future reviewed
  implementation may delete the exact trigger with
  `gcloud eventarc triggers delete "$ECHO_TRIGGER_URI" --project="$PROJECT"
  --location="$REGION"` only when its Terraform recreation, delivery semantics,
  and service drain are already tested and signed. Today this operation is not
  authorized.
- **Cloud Run Jobs:** enumerate all executions for the exact full Job URI,
  cancel each nonterminal exact execution with
  `gcloud run jobs executions cancel "$EXECUTION_URI" --project="$PROJECT"
  --region="$REGION" --no-async`, wait for terminal state, and prove the list
  contains zero pending/running executions. This drains but does not suppress;
  the reviewed execution-disable seam is still mandatory.
- **Cloud Run services:** provider scaling is not an absence primitive. The
  future no-database revision, routing/IAM fence, and timeout-bounded drain must
  all pass. An HTTP error alone is not process absence.

## Accountability, resource binding, and evidence hygiene

### Signed immutable manifest

Before any mutation, two reviewers sign one canonical manifest containing:

| Binding | Required value |
|---|---|
| change | incident/change ID, approved half-open UTC window, commander |
| identities | restore operator, endpoint operator, two reviewers |
| CLI | named gcloud configuration, active account, SDK version hash |
| location | project ID and number, region |
| source | full cluster URI, primary URI, network URI, PSA range, KMS key URI or explicit Google-managed encryption |
| restored | reserved distinct cluster and primary URIs, target PITR UTC |
| code | full public/deployment SHAs and immutable image digests |
| consumers | all eight full resource URIs and replacement template/revision URIs |
| jobs | full Job URIs, expected generations, task/retry/timeout/service-account/VPC/image contracts |
| SQL | committed SHA-256 for the four SID operation objects, runtime check, schema and privilege manifests |
| credentials | admin and worker Secret Manager **numeric version resource IDs only** |
| data | reviewed SID count `19`, Feed count `154`, and exact 64-hex manifest digest |
| fallback | retention owner, deadline, and separate destruction approval path |

The account, project, region, full resource URI, UID/generation, and signed
manifest hash must agree before every mutating command. Never rely on an active
gcloud default, a short resource name, Terraform state alone, or a name copied
from a previous incident.

### Bounded descriptions before mutation

Describe every bound resource before mutation, but retain only reviewed bounded
fields. For example, the selected cluster projection may include full name, UID,
database version, network, allocated range, backup bounds, encryption mode/key
URI, and labels. The selected primary projection may include full name, UID,
state, availability, CPU, database flags, pooling enabled/mode/limits, and
query-insights settings. Cloud Run projections may include full name, UID,
generation, service account, image digest, VPC connector/interface, timeout,
task/retry count, env **key names**, endpoint SHA-256, and numeric secret-version
IDs.

Pipe provider output directly through the reviewed reducer and hash the bounded
projection. Do not save an intermediate `--format=json` response. Do not use
`--log-http`. Reject a null, duplicate, truncated, or unexpected field.

### Secret-safe shell boundary

At the start of an approved session:

```sh
set +x
umask 077
```

Do not run `env`, `set`, `printenv`, or a full service/job description into the
evidence store. Do not put a database password in an argument, environment
override, temporary file, shell history, or log. Do not call `gcloud secrets
versions access` from the operator shell. The reviewed probe/operation surfaces
must reference the signed numeric Secret Manager versions directly.

Freeze secret rotation and every deploy/apply path before Gate 0. Re-read only
the numeric version metadata (never payloads) before endpoint apply, before SQL,
and before startup. A changed, destroyed, disabled, or newly selected version is
a stop condition.

In this runbook, **current Secret Manager credentials** means the two numeric
version resource IDs signed at `DR_PREPARED`, not the mutable `latest` alias.

## Command-version preflight

The command shapes below were reviewed against Google Cloud SDK 565.0.0 and
current Google Cloud SDK documentation. At execution time, repeat the official
documentation review for the installed version and retain a bounded version
projection/hash. Stop if current help differs:

```sh
gcloud alloydb clusters restore --help
gcloud alloydb clusters describe --help
gcloud alloydb instances create --help
gcloud alloydb instances describe --help
gcloud run jobs execute --help
gcloud run jobs executions list --help
gcloud run jobs executions describe --help
gcloud run jobs executions cancel --help
gcloud eventarc triggers delete --help
gcloud scheduler jobs pause --help
```

Use a named gcloud configuration scoped to the signed operator account. Every
command must include the signed `--project` and `--region`/`--location`; implicit
defaults are forbidden.

## Future state machine

Only the incident commander advances a state after an operator and an
independent reviewer sign its evidence. An abort preserves both external fences
and leaves restored claims stopped.

### DR_PREPARED

**Entry gate:** Gate 0 has been implemented, reviewed, tested, and rehearsed in
non-production. Incident/change ownership, two reviewers, infrastructure/app/
membership/credential freezes, exact commits/images, recovery target, fallback
period, signed resource manifest, and evidence location are approved.

**Mutation:** Read only. Bind the active account/project/region and full resource
URIs. Capture bounded source cluster, primary, backup, network/PSA, KMS,
credentials metadata, and all-eight-consumer projections. Confirm
`TARGET_PITR_UTC` is RFC 3339 UTC, is no earlier than
`continuousBackupInfo.earliestRestorableTime`, and precedes the incident.

**Expected bounded output:** Exactly one reviewed source cluster and primary;
one network/PSA topology; one explicit encryption contract; continuous backup
available at the target; one current active endpoint hash; eight discovered
consumer rows; two unchanged numeric credential versions; and distinct reserved
restored resource URIs.

**Evidence:** Signed manifest/hash, bounded projection object IDs/hashes,
operator, reviewer, and UTC. No full provider response or endpoint value.

**Stop condition:** Gate 0 still blocked, missing backup, target outside bounds,
unknown network/PSA/encryption, unreviewed topology/config delta, `latest` secret
reference, endpoint ambiguity, queued deployment, unbound resource, or any HIGH
finding.

**Abort/fallback:** No resource has changed. Resolve the prerequisite or use a
separately approved recovery method.

### OLD_TIMELINE_FENCED

**Entry gate:** `DR_PREPARED` passes. The signed eight-row ledger and complete
process inventory are current. Original autoscaler, autohealing, repair, boot,
restart, trigger, invoker, job-execution, template, image, secret, and endpoint
controls are recorded.

**Mutation:** Execute only the reviewed Gate 0 fence plan. For the collector MIG,
reuse the exact complete external process fence from the SID cutover runbook.
For Cloud Run services, triggers, Scheduler, and Jobs, execute the reviewed
provider-specific suppression and drain controls. Cancel exact nonterminal Job
executions only after they are bound to the signed ledger. Wait each service's
full maximum request/task timeout and the implementation's delivery-drain
interval. Re-describe every resource and replacement template.

**Expected bounded output:** All eight ledger rows are `REPROVED`; every
nonterminal Job execution count is zero; every DB-capable service route and
invocation path is suppressed; the old MIG's exact slots are absent; every
replacement template remains fenced; no restore resource exists.

**Evidence:** Complete ledger, terminal provider operation IDs, pre/post bounded
hashes, exact process-absence object/hash, drain interval, operator/reviewer/UTC.

**Stop condition:** Any partial row, new invocation/execution, scheduler still
enabled, trigger ambiguity, DB-capable revision route, numeric-ID replacement,
SSH-unreachable VM, >120-second worker, changed secret version, queued deploy,
or unknown process/resource. Missing is unknown unless deletion is the signed
control and provider terminal deletion is proved.

**Abort/fallback:** Restore original controls only if the incident commander
abandons recovery and the unchanged source topology is independently
revalidated. Otherwise keep all eight consumers fenced.

### RESTORED_UNCLAIMED

**Entry gate:** `OLD_TIMELINE_FENCED` is independently reproved immediately
before restore. The source/restored full URIs are distinct. Project, region,
network/PSA, target, KMS mode/key, sizing, availability, flags, pooling values,
and every approved delta are signed.

**Mutation:** Perform an out-of-place restore. Use the full source cluster URI:

```sh
gcloud alloydb clusters restore "$RESTORED_CLUSTER_ID" \
  --source-cluster="$SOURCE_CLUSTER_URI" \
  --point-in-time="$TARGET_PITR_UTC" \
  --network="$SOURCE_NETWORK_URI" \
  --allocated-ip-range-name="$SOURCE_PSA_RANGE" \
  --project="$PROJECT" --region="$REGION"
```

If the signed source uses CMEK, include the exact signed full
`--kms-key="$SOURCE_KMS_KEY_URI"`. If it uses Google-managed encryption, omit
the KMS flag and record that equality explicitly. Never silently change
encryption mode.

After the restore operation succeeds, create one primary. For the current
pooling-enabled production contract, the positive enable flag and all three
pool settings are mandatory. Normalize the reviewed pool mode to the SDK enum
`TRANSACTION` before signing the command:

```sh
gcloud alloydb instances create "$RESTORED_PRIMARY_ID" \
  --cluster="$RESTORED_CLUSTER_ID" \
  --instance-type=PRIMARY \
  --cpu-count="$PRIMARY_CPU_COUNT" \
  --availability-type="$PRIMARY_AVAILABILITY_TYPE" \
  --database-flags="$PRIMARY_DATABASE_FLAGS" \
  --enable-connection-pooling \
  --connection-pooling-pool-mode="$PRIMARY_POOL_MODE" \
  --connection-pooling-max-pool-size="$PRIMARY_MAX_POOL_SIZE" \
  --connection-pooling-max-client-connections="$PRIMARY_MAX_CLIENT_CONNECTIONS" \
  --project="$PROJECT" --region="$REGION"
```

Only if the signed source projection proves pooling disabled may the reviewed
command use `--no-enable-connection-pooling` and omit every pooling-tuning flag.
If the exact reviewed database-flag map is empty, omit `--database-flags`
instead of passing an empty value.

Describe source and restored resources through bounded reducers. Assert:

- source/restored cluster full URIs and UIDs differ;
- source/restored primary full URIs and UIDs differ;
- project, region, network, PSA, encryption, database version, availability,
  flags, and pooling contract are equal unless a specific signed delta exists;
- the only implicit topology delta is the new out-of-place cluster/primary
  identity and private endpoint; and
- no consumer endpoint, Job, SQL operation, credential, or claim has changed.

Do not run database probes yet. Controlled admin and worker probes occur only
after the endpoint and Job templates converge on the restored primary.

**Expected bounded output:** One distinct healthy restored cluster and primary
in the reviewed topology, source unchanged, all eight consumers still fenced
to the old endpoint, and zero restored claimant.

**Evidence:** Restore/create operation IDs, bounded source/restored description
hashes, explicit equality/delta matrix, and fence reproof. Never retain the
private endpoint; retain its SHA-256 and full primary resource URI.

**Stop condition:** Restore/create failure, identity conflation, network/PSA/KMS
mismatch, unreviewed config delta, consumer change, secret rotation, or any
claim/invocation.

**Abort/fallback:** Keep all consumers and both timelines fenced. Restored
resources remain isolated for diagnosis; no endpoint or SQL change occurs.

### RESTORED_ENDPOINT_READY

**Entry gate:** `RESTORED_UNCLAIMED` passes. All eight fence rows are reproved.
The saved Terraform plan changes one durable
`active_alloydb_primary_instance_ip` from source to restored while retaining a
distinct managed-source output. The plan keeps
`database_recovery_maintenance_mode` enabled, pins both numeric credential
versions, changes no claim/start control, and cannot execute schema or operation
Jobs as a side effect.

**Mutation:** Review and apply that exact saved plan while all consumers remain
fenced. Then attest the live endpoint fan-out for all eight consumers and every
replacement template:

1. `audio_segments_api`
2. `bcfy_calls_sid_operation`
3. `collector_mig`
4. `echo_ingestion`
5. `feed_store`
6. `oldest_feed_publisher`
7. `rules_management`
8. `schema_migration`

For each row, compare the endpoint in memory to the restored primary and emit
only `consumer_id`, full resource URI, UID/generation, endpoint SHA-256, numeric
secret-version URI, service account URI, image digest, VPC binding, and
`fence_state=REPROVED`. Scan the rendered graph for a direct source endpoint,
stale literal, alternate DB variable, old revision/tag, or bypass. Prove the
source and active endpoint hashes are distinct.

Only after eight-of-eight convergence may the two reviewed probe surfaces run:

- **direct administrative probe:** port **5432**, expected
  `current_user=postgres`, admin numeric secret version;
- **pooled worker probe:** port **6432**, expected `current_user=worker`, worker
  numeric secret version and the signed pool mode/limits.

Both probes must be immutable, one task, zero retries, timeout-bounded, VPC-bound
to the signed network/subnetwork, and execution-disabled outside the reviewed
one-shot control. Each executes only a bounded query projecting
`current_user`, `current_database()`, `server_version_num`, the presence of
`public.ingestion_leases`, and its reviewed column count/schema revision. The
probe must not select an endpoint, password, connection string, row payload, or
unbounded catalog. Retain the exact execution URI, terminal result, bounded
projection, and SHA-256.

**Expected bounded output:** Saved apply succeeds; source/active outputs remain
distinct; eight-of-eight endpoints/templates select the restored endpoint while
remaining fenced; direct 5432 and pooled 6432 identity/schema projections match;
no schema/data operation and no claim has occurred.

**Evidence:** Plan/apply IDs and hashes, eight-row convergence matrix, two exact
probe execution URIs, numeric credential-version URIs, bounded query hashes, and
fence reproof.

**Stop condition:** Failed durable Terraform apply, any endpoint fan-out miss,
source conflation, `latest` secret, credential mismatch, wrong SQL identity,
direct/pooled failure, auto-executed migration, unreviewed revision/template, or
any consumer start.

**Abort/fallback:** Keep all processes fenced. Correct and re-plan, or, only if
the incident commander has separately classified the source as usable, apply a
reviewed all-eight rollback plan selecting it. No restored claimant may start
during either path.

### RESTORED_DATA_CLASSIFIED

**Entry gate:** `RESTORED_ENDPOINT_READY` passes. Both database probes identify
the restored timeline. All eight fence rows are reproved immediately before the
operation. The controlled operation Job's full URI, UID/generation, immutable
image, one-task/zero-retry/300-second contract, service account, VPC, DB host
hash, direct port 5432, numeric admin secret version, and execution-disable
sentinel match the signed manifest.

The four GCS operation objects (`001_preseed.sql`, `002_activate.sql`,
`003_rollback_children.sql`, `004_verify.sql`) and the referenced runtime-column
check must be frozen. Record each full `gs://` object name, generation, size,
CRC32C, and SHA-256 of its committed content. For a pre-schema branch, also
freeze and hash the complete ordered schema/privilege object manifest. A changed
generation or digest stops execution.

**Mutation:** Run only a generation-bound controlled Job. Never execute an
operation by an unqualified Job name. A representative invocation shape is:

```sh
T0_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
EXECUTION_URI="$(
  gcloud run jobs execute "$SID_OPERATION_JOB_URI" \
    --project="$PROJECT" --region="$REGION" \
    --tasks=1 --task-timeout=300s \
    --args=bcfy-calls-sid-operation,verify \
    --wait --format='value(metadata.name)'
)"
test -n "$EXECUTION_URI"
```

The execution is accepted only if its terminal description proves the signed
Job UID/generation, one task, zero retries, exact args, exact VPC/service account,
immutable image, endpoint hash, numeric secret version, and successful terminal
condition. Retain only that bounded projection/hash and the bounded SQL report.

Select exactly one branch:

1. **activated rows:** run `004_verify.sql`; preserve every Lease owner,
   lifecycle, failure history, membership revision, progress field, cursor, and
   fencing token. Ordinary stale-owner recovery may later acquire with a newer
   fence. Never pre-normalize active/failing/quarantined rows.
2. **dormant pre-seed rows:** run `004_verify.sql` and prove the exact dormant
   Lease state. If this recovery intends to enable SID authority, execute only
   `002_activate.sql` with `19`, the exact reviewed 64-hex manifest digest, and
   `CONFIRMED`, then run `004_verify.sql` again. A deliberately dormant row is
   never stale-recovered.
3. **pre-schema:** execute the exact controlled schema-migration generation and
   its ordered migrations, prove the ordered manifest and terminal result, then
   execute checked `001_preseed.sql`; if SID authority is intended, execute
   checked `002_activate.sql`; finally run `004_verify.sql`. A pre-schema restore
   never skips directly to claims.

The current schema-migration template is one task with `max_retries=1`; a single
provider execution can therefore contain at most two attempts. That bounded
provider retry is part of the signed execution. It is not permission to launch a
second execution. A terminal schema-migration failure can leave an idempotent
prefix applied and must be classified by bounded catalog/004 verification.

Every branch, including a branch that remains dormant, must finish with
`004_verify.sql` reporting exactly **19 SIDs**, **154 Feeds**, and the exact
reviewed manifest digest from the signed incident manifest. The 19/154 values
are not adjustable incident inputs. A different restored membership is a stop
condition requiring a new recovery decision, not an edited SQL argument.

Across all branches, **never mass-reset** owner, status, failure count/reason,
membership revision, cursor/progress, Lease identity, or fence. Preserve durable
history and use only the committed operations.

#### Returned failure versus unknown outcome

- If the exact SID-operation execution URI is returned and its terminal
  condition reports an assertion failure inside `001`, `002`, or `003`,
  `psql -v ON_ERROR_STOP=1` and that operation's transaction make it a known
  failed/rolled-back attempt. Record the assertion, keep every fence,
  investigate, and require a new reviewed authorization. Do not rerun
  automatically. A schema-migration failure is known failed but is not assumed
  rolled back across files; classify its possibly applied idempotent prefix with
  bounded read-only verification.
- If the client loses transport, times out, or returns no execution URI, the
  outcome is unknown. Do not issue another mutating execution. Using `T0_UTC`
  and the exact Job URI, list bounded execution names/timestamps created since
  T0. Exactly one candidate may be described and followed to terminal state.
  Zero or multiple candidates remain unknown and stop the runbook.
- After any unknown mutating outcome, run the read-only `004_verify.sql` surface
  only after the unique execution is terminal or provider cancellation is
  terminal. Compare complete expected state. Never use a blind retry to discover
  whether activation or migration was consumed.

**Expected bounded output:** One classified branch, exact terminal execution
URIs, 004 verification at 19/154 with the signed digest, intended explicit
migration/pre-seed/activation effects only, preserved history, and zero claims.

**Evidence:** Branch decision, frozen SQL object generations/hashes, Job bounded
projection, exact execution URIs, terminal results, before/after bounded reports,
final manifest report/hash, reviewer and UTC.

**Stop condition:** Ambiguous schema/branch, digest mismatch, unexplained durable
delta, dormant stale recovery, wrong endpoint/secret/Job generation, failed
assertion, nonterminal execution, or unknown outcome. A last activation
assertion failure is not permission to retry.

**Abort/fallback:** Keep both timelines externally fenced and investigate with
bounded read-only reports. Do not repair uncertainty with broad updates.

### RESTORED_ACTIVE

**Entry gate:** Reprove complete old-timeline and restored maintenance absence.
`RESTORED_ENDPOINT_READY` remains eight-of-eight. `RESTORED_DATA_CLASSIFIED`
passes its final 004 report. Endpoint, credentials, authority mode, profile,
image, and replacement templates agree. No deploy, MIG action, trigger delivery,
or Job execution is queued.

**Mutation:** Move out of maintenance in explicit groups, re-attesting the
restored endpoint and source absence between groups:

1. enable only `audio_segments_api`, `feed_store`, and `rules_management` on
   their already-proved restored revisions;
2. start the collector exactly once on the same reviewed slots, observe ordinary
   claims with fencing tokens newer than restored durable state, and run the
   exact 19/19 bootstrap plus post-bootstrap 30-minute soak gates from the SID
   cutover runbook;
3. after acceptance, enable Echo service/Eventarc and the Publisher
   service/Scheduler on their restored revisions; and
4. restore normal Job invocation, autoscaler, autohealing, repair, boot, restart,
   and deployment controls only after proving no queued execution/deploy and
   re-attesting the complete live topology.

`bcfy_calls_sid_operation` returns to its read-only default and
`schema_migration` to its reviewed normal trigger contract; neither is executed
merely to prove it is enabled. If any group fails, re-fence the already-enabled
restored groups before evaluating fallback.

**Expected bounded output:** One live database timeline; monotonic fences; exact
19/19 bootstrap; passing soak; all eight intended consumers on restored; source
and old fleet still externally fenced.

**Evidence:** Startup/fence rows, bootstrap/soak raw-log object IDs and hashes,
reducer results, all-eight topology proof, control-restoration report, and signed
acceptance decision.

**Stop condition:** Any old process, stale endpoint, non-monotonic fence,
bootstrap/soak failure, incomplete consumer, credential error, unexpected Job,
or durable/runtime divergence. Stop restored authority before fallback; never
run both timelines to compare them.

**Abort/fallback:** Re-enter complete no-authority absence for all eight
consumers. While both timelines are fenced, select source only through a reviewed
all-eight endpoint rollback plan, revalidate source credentials/data, and start
one timeline once.

### FALLBACK_HOLD_COMPLETE

**Entry gate:** `RESTORED_ACTIVE` passes its approved observation period and the
incident commander accepts the restored timeline.

**Mutation:** Retain the source cluster and old fleet fenced for the approved
fallback period. At expiration, use the organization's separate retention and
resource-lifecycle process. This runbook does not authorize source cleanup.

**Expected bounded output:** One active restored timeline, one fenced retained
source, normal controls restored only on active topology, and an accountable
fallback-expiry decision.

**Evidence:** Final topology hashes, control states, retention owner/deadline,
operator/reviewer/UTC.

**Stop condition:** Consumer drift, unowned fallback deadline, missing evidence,
or any HIGH finding keeps the fallback fence in place.

**Abort/fallback:** Extend fenced retention under incident ownership.

## Evidence index

Append a PITR section to the cutover evidence index containing only:

- incident/change ID, official-document review timestamp, bounded SDK version
  hash, and signed manifest hash;
- source/restored full resource URIs, UIDs, project/region/network/PSA/KMS
  bindings, and bounded description hashes;
- target UTC and bounded continuous-backup proof;
- all-eight durable fence/convergence ledger and exact provider operation IDs;
- restore/create operation IDs and topology equality/delta matrix;
- numeric Secret Manager version resource IDs and direct/pooled query hashes;
- frozen SQL object generation/digest manifest, exact Job execution URIs, branch,
  and final 19/154 manifest digest;
- startup/fence, bootstrap, soak, control-restoration, and fallback-hold evidence;
  and
- operator, reviewer, commander, UTC, decisions, and exceptions.

Do not retain private endpoint values, password material, full environments,
signed URLs, bearer tokens, connection strings, or unbounded provider payloads.
The evidence index must retain the literal status `PITR REVIEWED — NOT
EXERCISED` until a separately approved live restore, endpoint switch, data
classification, start, soak, fallback, and complete external-fence exercise
actually passes.

## Tabletop failure matrix

Tabletop validates fail-closed branches; it does not replace Gate 0
implementation or a non-production rehearsal.

| Injection | Required response | Opposite/restored authority |
|---|---|---|
| Gate 0 consumer lacks durable fence | abort in `DR_PREPARED`; no restore/endpoint/SQL | stopped |
| Cloud Run service scales to zero but remains invocable | reject fence row; scaling is not absence | stopped |
| Eventarc trigger deleted without reviewed recreation/drain | reject fence row | stopped |
| Scheduler paused but Publisher service remains invocable | reject fence row | stopped |
| pending/running Job execution | cancel exact execution, wait terminal, reprove; no new execution | stopped |
| Job or consumer resolves secret `latest` | abort before endpoint/SQL | stopped |
| wrong restored credential or SQL identity | stop in `RESTORED_ENDPOINT_READY` | stopped |
| endpoint fan-out miss | reject readiness and retain all fences | stopped |
| failed durable Terraform apply | retain fences; review source rollback or corrected plan | stopped |
| pre-schema restore | controlled migrations, pre-seed/activation as intended, then 004 at exact 19/154/digest | stopped |
| dormant-row restore | 004 proves dormancy; checked activation only after explicit intent | stopped |
| returned activation assertion rollback | record known failure; investigate; no automatic retry | stopped |
| transport loss/unknown Job outcome | discover unique post-T0 execution, follow terminal, read-only verify; no blind retry | stopped |
| network/PSA/KMS mismatch | isolate restored resource; no endpoint/SQL | stopped |
| unavailable old VM or unknown consumer | treat as ambiguity, never absence | stopped |

### Independent review 1 — external authority and topology

Reviewer verifies Gate 0 implementation, all-eight suppression/drain/absence,
complete trigger/invoker/execution inventory, restart/redeploy suppression,
resource binding, endpoint-before-SQL ordering, one-timeline start, and fallback.
Any HIGH finding blocks execution.

**Reviewer:** ____  **UTC:** ____  **Findings/resolutions:** ____

### Independent review 2 — data, Job, and credential integrity

Reviewer verifies backup bounds, network/PSA/KMS/config equality, direct 5432 and
pooled 6432 identity probes, numeric secret pins, exact Job generations and SQL
object generations/hashes, activated/dormant/pre-schema branches, final
004 19/154/digest proof, unknown-outcome handling, and secret-safe evidence.

**Reviewer:** ____  **UTC:** ____  **Findings/resolutions:** ____

Both independent reviews must conclude **no unmitigated HIGH** finding before a
future execution may proceed.

## Publication assertions

This reviewed procedure intentionally makes no live-recovery claim:

`PITR REVIEWED — NOT EXERCISED`

`NO PRODUCTION CUTOVER PERFORMED`

`LIVE PITR BLOCKED UNTIL GATE 0 IS IMPLEMENTED AND REHEARSED`
