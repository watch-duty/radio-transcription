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
execution, and every replacement template. A separate internal-automation
fence must stop database-resident writers such as `pg_cron`; they have no
external process to stop.

The source remains fenced throughout restore, endpoint convergence, data
classification, validation, startup, and the approved fallback period.
Restored claims remain disabled until:

1. all eight old-timeline consumer rows are durably suppressed, drained, absent,
   and independently reproved;
2. all eight replacement definitions converge on one selected restored endpoint
   while still fenced;
3. controlled direct and pooled probes identify the restored database;
4. the selected data branch passes `004_verify.sql` with the exact reviewed
   19-SID/154-Feed manifest digest; and
5. the separate internal-automation ledger remains inert and independently
   reproved.

Database rows, Lease heartbeats, logs, tests, and a tabletop are diagnostics;
none proves cross-timeline process absence.

## Gate 0: current implementation blocker

The checked-in deployment graph has two partial external stop/binding seams and
six missing external seams. It also has an unfenced internal writer. The table
below is authoritative for the eight **external endpoint consumers**. A row
marked `BLOCKED` cannot be completed by an improvised `gcloud` command.

| # | Endpoint consumer | Checked-in execution surface | Present control | Gate 0 result |
|---:|---|---|---|---|
| 1 | `audio_segments_api` | Cloud Run v2 service | no no-database maintenance revision, complete invoker fence, or drain proof | **BLOCKED** |
| 2 | `bcfy_calls_sid_operation` | manually invoked Cloud Run v2 Job | numeric Secret Manager version is pinned; no durable execution-disable sentinel | **BLOCKED** |
| 3 | `collector_mig` | regional MIG, two worker slots per VM | process stop is available through `ingestion_maintenance_mode` and the exact absence procedure, but the template embeds `ALLOYDB_PASSWORD` as a raw Terraform/user-data value rather than a numeric immutable secret binding | **INCOMPLETE — BLOCKED** |
| 4 | `echo_ingestion` | Eventarc trigger to a min-one Cloud Run v2 service | Eventarc has delete but no pause; no checked-in reversible trigger/service fence or drain proof | **BLOCKED** |
| 5 | `feed_store` | Cloud Run v2 service | no no-database maintenance revision, complete invoker fence, or drain proof | **BLOCKED** |
| 6 | `oldest_feed_publisher` | Cloud Scheduler to Cloud Run v2 service | Scheduler can be paused, but the service remains invocable and may have an in-flight request | **INCOMPLETE — BLOCKED** |
| 7 | `rules_management` | Cloud Run v2 service | no no-database maintenance revision, complete invoker fence, or drain proof | **BLOCKED** |
| 8 | `schema_migration` | Terraform-triggered and manually invocable Cloud Run v2 Job | numeric Secret Manager version is pinned; no durable execution-disable sentinel; Terraform can invoke it | **BLOCKED** |

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

### Separate Gate 0 blocker: database-internal automation

The external table deliberately remains eight rows. It does not include code
that runs inside AlloyDB itself. Production enables
`alloydb.enable_pg_cron=on` and pins `cron.database_name=postgres`. Migration
`019_feeds_pg_cron_jobs.sql` then:

- creates the `pg_cron` extension;
- schedules `feeds-abandoned-lease-sweep` every 30 seconds; that Job locks up to
  500 non-Echo `public.feeds` rows whose active heartbeat is older than 60
  seconds and mutates status, owner, and `unclaimed_since`; and
- schedules `feeds-vac` every minute to execute `VACUUM public.feeds`.

`cron.schedule` is name-idempotent here: applying migration 019 again updates
the named Job rather than creating a safe inactive duplicate. Stopping every
external consumer therefore still leaves a database-resident writer. Copying
the source flags to a restored primary can start that writer before endpoint
proof or data classification. Keeping the source for fallback also leaves it
mutating unless its internal automation is fenced.

Current deployment has no reviewed recovery control for these Jobs and no
recovery-safe migration mode that prevents migration 019 from scheduling them.
This independently blocks live PITR before restore.

### Implementation required to clear Gate 0

A future reviewed deployment change must add one coherent
`database_recovery_maintenance_mode` (name may differ, semantics may not) that:

- keeps the existing collector MIG maintenance controls and exact slot-level
  absence proof, and changes the MIG template to fetch/reference the signed
  numeric worker Secret Manager version (or an independently reviewed immutable
  non-payload binding) instead of embedding the raw password;
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
- preserves the Jobs' numeric Secret Manager pins and adds equivalent numeric
  bindings to every recovery consumer for the complete recovery window;
- adds a separately reviewed internal-automation fence that disables and drains
  the exact source `pg_cron` Jobs, holds a restored primary with
  `alloydb.enable_pg_cron=off` (or an equivalently proved inert setting), and
  prevents migration 019 from scheduling/enabling Jobs before final activation;
- provides durable IaC ownership for the restored cluster and primary while
  preserving the internal-automation hold;
- adds a tested canonical 004-output reducer/comparator that exits nonzero on a
  count, digest, duplicate, or malformed-report mismatch;
- keeps all replacement templates fenced while the endpoint changes; and
- has deterministic tests for all eight rows, including restart/redeploy
  suppression and a fail-closed unknown-resource branch.

The implementation review must produce a deployment commit, an immutable image
digest, exact resource bindings, and a successful non-production rehearsal.
Only then may an incident-specific runbook revision replace the `BLOCKED` cells
and internal-writer blocker with exact reviewed commands. Until that happens,
the remaining state machine is a future execution contract, not authorization
to mutate production.

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

### Separate internal-automation ledger

The signed recovery record must also contain an `INTERNAL_AUTOMATION` ledger.
It is not a ninth endpoint consumer. It binds both primary resource URIs,
migration 019's committed digest, `alloydb.enable_pg_cron`,
`cron.database_name`, and these exact named Jobs:

| Job | Reviewed schedule | Reviewed effect |
|---|---|---|
| `feeds-abandoned-lease-sweep` | `30 seconds` | at most 500 stale active non-Echo Feed lifecycle rows per run |
| `feeds-vac` | `* * * * *` | `VACUUM public.feeds` |

Before changing them, a bounded repeatable-read inventory must project exactly
one `cron.job` row per name: `jobid`, `jobname`, schedule, database, username,
active state, and SHA-256 of command text. It must also project, per exact
`jobid`, the maximum `cron.job_run_details.runid`, count of running rows, and
latest start/end/status; never retain command text or return messages. Unknown,
duplicate, extra reviewed-name, or inaccessible rows fail closed.

The future tested fence advances:

`DISCOVERED -> SCHEDULE_DISABLED -> RUNS_DRAINED -> ENGINE_DISABLED -> REPROVED`

Freeze schema deploy first so migration 019 cannot reschedule the Jobs. Use a
reviewed transaction to durably disable or unschedule exactly the two source
Jobs, wait for their exact run IDs to become terminal, and obtain two stable
bounded observations with no new run ID. Only then may external consumers be
stopped and the source primary be converged to a reviewed
`alloydb.enable_pg_cron=off` hold. The flag change/restart and its terminal
provider operation must be proved; row absence alone is insufficient.

Create the restored primary with that same inert hold even though source parity
normally enables pg_cron. This **is an explicit signed recovery safety delta**,
not a parity failure. Inventory restored `cron.job` and `cron.job_run_details`
through the controlled direct path as soon as the endpoint is ready, while the
engine remains disabled. Pre-schema recovery must use a reviewed migration mode
that cannot apply migration 019 early. If the all-or-nothing schema runner
cannot exclude or guard 019, pre-schema PITR stops.

Keep both source and restored internal ledgers `REPROVED` before restore,
endpoint apply, every probe/SQL operation, startup, and fallback decision. The
source stays disabled for the entire fallback hold. Restore the exact named
Jobs and enable their engine only on the one accepted timeline, after normal
workers are healthy; then compare names, schedules, databases, users, command
digests, flag state, and new run status to the signed manifest.

## Accountability, resource binding, and evidence hygiene

### Signed immutable manifest

Before any mutation, two reviewers sign one canonical manifest containing:

| Binding | Required value |
|---|---|
| change | incident/change ID, approved half-open UTC window, commander |
| identities | restore operator, endpoint operator, two reviewers |
| CLI | named gcloud configuration, active account, SDK version hash |
| location | project ID and number, region |
| source | full cluster URI, primary URI, network URI, PSA range, KMS key URI or explicit Google-managed encryption, automated and continuous backup contracts, deletion protection, durable IaC address, labels, and Query Insights |
| restored | reserved distinct cluster and primary URIs, target PITR UTC, durable IaC address, and signed parity/delta matrix |
| code | full public/deployment SHAs and immutable image digests |
| consumers | all eight full resource URIs and replacement template/revision URIs |
| jobs | full Job URIs, expected generations, task/retry/timeout/service-account/VPC/image contracts |
| SQL | committed SHA-256 for the four SID operation objects, runtime check, schema and privilege manifests, migration 019, and the tested canonical 004 reducer |
| internal automation | source/restored flag contracts and the exact two pg_cron Job identities/schedules/command digests/run frontiers |
| credentials | admin and worker Secret Manager **numeric version resource IDs only** |
| data | reviewed SID count `19`, Feed count `154`, and exact 64-hex manifest digest |
| fallback | retention owner, deadline, and separate destruction approval path |

The account, project, region, full resource URI, UID/generation, and signed
manifest hash must agree before every mutating command. Never rely on an active
gcloud default, an **unvalidated** short resource name, Terraform state alone,
or a name copied from a previous incident. When the GA gcloud surface requires
a short resource ID, first validate its expected full URI mechanically and
always pass the signed `--project` and `--region`; only that combination is a
qualified CLI reference.

### Bounded descriptions before mutation

Describe every bound resource before mutation, but retain only reviewed bounded
fields. For example, the selected cluster projection may include full name, UID,
database version, network, allocated range, backup bounds, encryption mode/key
URI, and labels. The selected primary projection may include full name, UID,
state, availability, CPU, database flags, pooling enabled/mode/limits, and
query-insights settings. Cloud Run projections may include full name, UID,
generation, service account, image digest, VPC connector/interface, timeout,
task/retry count, env **key names**, endpoint SHA-256, and numeric secret-version
IDs. The MIG projection likewise retains only env key names and the immutable
worker-secret binding. Never compute or retain a hash of the raw password, a
rendered full environment, or complete user data; a password hash is still
credential-derived evidence and is forbidden.

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
independent reviewer sign its evidence. An abort preserves both external fences,
the internal-automation hold, and restored claims stopped.

### DR_PREPARED

**Entry gate:** Gate 0 has been implemented, reviewed, tested, and rehearsed in
non-production. Incident/change ownership, two reviewers, infrastructure/app/
membership/credential freezes, exact commits/images, recovery target, fallback
period, signed resource manifest, canonical 004 reducer, external and internal
fence implementations, and evidence location are approved.

**Mutation:** Read only. Bind the active account/project/region and full resource
URIs. Capture bounded source cluster, primary, backup, network/PSA, KMS,
automated-backup policy, continuous-backup window, deletion protection, labels,
Query Insights, credentials metadata, all-eight-consumer projections, and the
internal-automation inventory. Confirm
`TARGET_PITR_UTC` is RFC 3339 UTC, is no earlier than
`continuousBackupInfo.earliestRestorableTime`, and precedes the incident.

**Expected bounded output:** Exactly one reviewed source cluster and primary;
one network/PSA topology; one explicit encryption contract; continuous backup
available at the target; one current active endpoint hash; eight discovered
consumer rows; an exact two-Job internal inventory; two unchanged numeric
credential versions; and distinct reserved restored resource URIs/IaC addresses.

**Evidence:** Signed manifest/hash, bounded projection object IDs/hashes,
operator, reviewer, and UTC. No full provider response or endpoint value.

**Stop condition:** Gate 0 still blocked, missing backup, target outside bounds,
unknown network/PSA/encryption, unreviewed topology/config delta, `latest` secret
reference, raw MIG credential binding, endpoint ambiguity, unknown internal
automation, queued deployment, unbound resource, or any HIGH finding.

**Abort/fallback:** No resource has changed. Resolve the prerequisite or use a
separately approved recovery method.

### OLD_TIMELINE_FENCED

**Entry gate:** `DR_PREPARED` passes. The signed eight-row ledger and complete
process inventory are current. The signed internal-automation ledger identifies
exactly the two reviewed cron Jobs. Original autoscaler, autohealing, repair,
boot, restart, trigger, invoker, job-execution, template, image, secret,
endpoint, pg_cron, backup, and IaC controls are recorded.

**Mutation:** Freeze schema deploy, execute the reviewed exact-Job disable
transaction, drain source cron run IDs, and prove two stable observations before
stopping external consumers. Then execute only the reviewed external Gate 0
fence plan. For the collector MIG, reuse the exact complete external process
fence from the SID cutover runbook. For Cloud Run services, triggers, Scheduler,
and Jobs, execute the reviewed provider-specific suppression and drain controls.
Cancel exact nonterminal Job executions only after binding them to the signed
ledger. Wait each service's full maximum request/task timeout and delivery-drain
interval. Converge the source instance to the signed pg_cron-disabled flag set,
wait through its restart, and re-describe every resource/template.

This complete external process fence and the separate internal-automation fence
must both pass; neither substitutes for the other.

**Expected bounded output:** All eight ledger rows are `REPROVED`; every
nonterminal Job execution count is zero; every DB-capable service route and
invocation path is suppressed; the old MIG's exact slots are absent; every
replacement template remains fenced; no restore resource exists.
The source internal ledger is `REPROVED`: both schedules are disabled, no exact
run is nonterminal or newly created, and the source engine flag is off.

**Evidence:** Complete external and internal ledgers, terminal provider operation
IDs, pre/post bounded hashes, exact process-absence object/hash, cron run
frontiers/flag operation, drain intervals, operator/reviewer/UTC.

**Stop condition:** Any partial row, new invocation/execution, scheduler still
enabled, trigger ambiguity, DB-capable revision route, numeric-ID replacement,
SSH-unreachable VM, >120-second worker, changed secret version, raw MIG password,
new/nonterminal cron run, pg_cron still enabled, queued deploy, or unknown
process/resource. Missing is unknown unless deletion is the signed control and
provider terminal deletion is proved.

**Abort/fallback:** Restore original controls only if the incident commander
abandons recovery and the source topology/internal Jobs are independently
revalidated. Otherwise keep all eight consumers and source pg_cron fenced.

### RESTORED_UNCLAIMED

**Entry gate:** `OLD_TIMELINE_FENCED` is independently reproved immediately
before restore. The source/restored full URIs are distinct. Project, region,
network/PSA, target, KMS mode/key, sizing, availability, hold flags, pooling,
Query Insights, backup policies, deletion protection, labels, durable IaC
addresses, and every approved delta are signed. The source internal ledger is
still `REPROVED`.

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
`TRANSACTION` before signing the command. `RESTORED_HOLD_DATABASE_FLAGS` must
equal the reviewed source map except for the explicit safety delta
`alloydb.enable_pg_cron=off`:

```sh
gcloud alloydb instances create "$RESTORED_PRIMARY_ID" \
  --cluster="$RESTORED_CLUSTER_ID" \
  --instance-type=PRIMARY \
  --cpu-count="$PRIMARY_CPU_COUNT" \
  --availability-type="$PRIMARY_AVAILABILITY_TYPE" \
  --database-flags="$RESTORED_HOLD_DATABASE_FLAGS" \
  --enable-connection-pooling \
  --connection-pooling-pool-mode="$PRIMARY_POOL_MODE" \
  --connection-pooling-max-pool-size="$PRIMARY_MAX_POOL_SIZE" \
  --connection-pooling-max-client-connections="$PRIMARY_MAX_CLIENT_CONNECTIONS" \
  --insights-config-query-string-length="$QUERY_INSIGHTS_STRING_LENGTH" \
  --insights-config-query-plans-per-minute="$QUERY_INSIGHTS_PLANS_PER_MINUTE" \
  --insights-config-record-application-tags \
  --insights-config-record-client-address \
  --project="$PROJECT" --region="$REGION"
```

Only if the signed source projection proves pooling disabled may the reviewed
command use `--no-enable-connection-pooling` and omit every pooling-tuning flag.
For this production topology the hold flag map is never empty; passing an empty
or source-normal flag map is a stop condition.

Before any endpoint or SQL operation, adopt the restored cluster and primary
into the signed durable IaC address using an exact saved plan. The plan must
enable deletion protection, converge the source's automated-backup schedule/
window/retention and continuous-backup recovery window/encryption, apply the
reviewed labels, preserve Query Insights, flags, and pooling, and keep the
pg_cron safety delta off. It must not replace/destroy either source or restored
resource and must not invoke a schema Job. A future implementation needs an
explicit restored-resource IaC surface; the current one-cluster module is not
sufficient evidence.

Describe source and restored resources through bounded reducers. Assert:

- source/restored cluster full URIs and UIDs differ;
- source/restored primary full URIs and UIDs differ;
- project, region, network, PSA, encryption, database version, availability,
  CPU, non-pg_cron flags, pooling, automated-backup policy, continuous-backup
  recovery window/encryption, deletion protection, labels, Query Insights, and
  durable IaC ownership are equal unless a specific signed delta exists;
- restored `alloydb.enable_pg_cron=off` differs intentionally from the signed
  source normal-state `on`, while `cron.database_name=postgres` remains pinned;
- the only implicit topology delta is the new out-of-place cluster/primary
  identity and private endpoint; and
- no consumer endpoint, Job, SQL operation, credential, or claim has changed.

Do not run database probes yet. Controlled admin and worker probes occur only
after the endpoint and Job templates converge on the restored primary.

**Expected bounded output:** One distinct healthy restored cluster and primary
in the reviewed topology, source retained in its signed fenced/pg_cron-off
state, all eight consumers still fenced to the old endpoint, source and restored
pg_cron engines disabled, restored backup/deletion/IaC policy converged, and zero
restored claimant.

**Evidence:** Restore/create operation IDs, bounded source/restored description
hashes, saved IaC plan/apply IDs, explicit equality/delta matrix, and external/
internal fence reproof. Never retain the private endpoint; retain its SHA-256
and full primary resource URI.

**Stop condition:** Restore/create failure, identity conflation, network/PSA/KMS
mismatch, backup/deletion/label/Query-Insights/IaC drift, pg_cron enabled,
unreviewed config delta, consumer change, secret rotation, or any claim/
invocation/internal run.

**Abort/fallback:** Keep all consumers and both timelines fenced. Restored
resources remain isolated for diagnosis; no endpoint or SQL change occurs.

### RESTORED_ENDPOINT_READY

**Entry gate:** `RESTORED_UNCLAIMED` passes. All eight fence rows and both
timeline sides of the internal-automation ledger are reproved.
The saved Terraform plan changes one durable
`active_alloydb_primary_instance_ip` from source to restored while retaining a
distinct managed-source output. The plan keeps
`database_recovery_maintenance_mode` enabled, pins both numeric credential
versions including the MIG worker binding, preserves both pg_cron-disabled
holds, changes no claim/start control, and cannot execute schema, migration 019,
or operation Jobs as a side effect.

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
source and active endpoint hashes are distinct. For the MIG, the proof is the
numeric secret-version resource binding—not a value or password hash.

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

The direct probe also performs the branch-aware internal inventory. For an
activated/dormant schema it must find exactly the signed two `cron.job` rows and
their bounded run frontiers while the restored engine remains off. For a true
pre-schema branch it must prove the `cron` schema/Jobs absent and the engine off.
In either case, the source internal ledger is reproved at the same boundary.

**Expected bounded output:** Saved apply succeeds; source/active outputs remain
distinct; eight-of-eight endpoints/templates select the restored endpoint while
remaining fenced; direct 5432 and pooled 6432 identity/schema projections match;
the branch-aware internal inventory matches; no schema/data operation, cron run,
or claim has occurred.

**Evidence:** Plan/apply IDs and hashes, eight-row convergence matrix, two exact
probe execution URIs, numeric credential-version URIs, bounded query hashes, and
external/internal fence reproof.

**Stop condition:** Failed durable Terraform apply, any endpoint fan-out miss,
source conflation, `latest` secret, credential mismatch, wrong SQL identity,
direct/pooled failure, auto-executed migration, pg_cron enabled/new run,
unreviewed revision/template, raw MIG credential, or any consumer start.

**Abort/fallback:** Keep all processes fenced. Correct and re-plan, or, only if
the incident commander has separately classified the source as usable, apply a
reviewed all-eight rollback plan selecting it. No restored claimant may start
during either path.

### RESTORED_DATA_CLASSIFIED

**Entry gate:** `RESTORED_ENDPOINT_READY` passes. Both database probes identify
the restored timeline. All eight fence rows and both internal-automation holds
are reproved immediately before the operation. The controlled operation Job's
full URI, UID/generation, immutable
image, one-task/zero-retry/300-second contract, service account, VPC, DB host
hash, direct port 5432, numeric admin secret version, and execution-disable
sentinel match the signed manifest.

The four GCS operation objects (`001_preseed.sql`, `002_activate.sql`,
`003_rollback_children.sql`, `004_verify.sql`) and the referenced runtime-column
check must be frozen. Record each full `gs://` object name, generation, size,
CRC32C, and SHA-256 of its committed content. For a pre-schema branch, also
freeze and hash the complete ordered schema/privilege object manifest. A changed
generation or digest stops execution.

**Mutation:** Run only a generation-bound controlled Job. The GA gcloud CLI
accepts the validated short Job ID for execute/list operations; it is qualified
only by the exact full-URI equality check and explicit signed project and region
shown below. Immediately before execution, describe the source Job through the
signed bounded reducer and retain its full URI, UID, generation, projection
SHA-256, and timestamp as `JOB_BEFORE`. A representative successful invocation
shape is:

```sh
EXPECTED_JOB_URI="projects/${PROJECT}/locations/${REGION}/jobs/${SID_OPERATION_JOB_ID}"
test "$SID_OPERATION_JOB_URI" = "$EXPECTED_JOB_URI"

T0_EPOCH="$(date -u +%s)"
T0_UTC="$(date -u -d "@$T0_EPOCH" +%Y-%m-%dT%H:%M:%SZ)"
EXECUTION_ID="$(
  gcloud run jobs execute "$SID_OPERATION_JOB_ID" \
    --project="$PROJECT" --region="$REGION" \
    --tasks=1 --task-timeout=300s \
    --args=bcfy-calls-sid-operation,verify \
    --wait --format='value(metadata.name)'
)"
case "$EXECUTION_ID" in
  ""|*/*|*[!a-z0-9-]*) exit 1 ;;
esac
EXECUTION_URI="${SID_OPERATION_JOB_URI}/executions/${EXECUTION_ID}"
test "$EXECUTION_URI" = \
  "projects/${PROJECT}/locations/${REGION}/jobs/${SID_OPERATION_JOB_ID}/executions/${EXECUTION_ID}"
POST_EXECUTION_EPOCH="$(date -u +%s)"
test "$POST_EXECUTION_EPOCH" -ge "$T0_EPOCH"
# The exclusive bound is a strict successor even when `date` has only
# second resolution and the execution starts/finishes within T0's second.
T1_EPOCH="$((POST_EXECUTION_EPOCH + 1))"
T1_UTC="$(date -u -d "@$T1_EPOCH" +%Y-%m-%dT%H:%M:%SZ)"
test "$T1_EPOCH" -gt "$POST_EXECUTION_EPOCH"
```

`value(metadata.name)` is only the returned execution ID; it is not accepted as
the canonical resource identity. `SID_OPERATION_JOB_URI` and
`SID_OPERATION_JOB_ID` must already be mutually validated against the signed
project/location/Job manifest before constructing the URI above.

Google Cloud SDK 565.0.0 exposes GA execution list/describe data in the
v1/Kubernetes shape: `metadata.*`, copied task-template fields, and `status.*`.
Invoke describe with the validated `EXECUTION_ID` plus explicit signed project
and region, then pipe its bounded projection directly to the reducer. The
description can prove the execution's immutable copied task template, creation
and completion times, and terminal conditions. It does **not** prove the source
Job's UID/generation; only `JOB_BEFORE` and `JOB_AFTER` establish that
continuity.

In parallel, list the validated short Job ID's bounded metadata/status fields
and pass them to the tested post-T0 discovery reducer for the half-open
`[T0_UTC, T1_UTC]` window. The `--job` selector is not itself parent proof. For
each row, the reducer must require the exact `run.googleapis.com/job` label,
require its value to equal `SID_OPERATION_JOB_ID`, derive the canonical parent
URI from the signed project, region, and label, and require that URI to equal
`SID_OPERATION_JOB_URI`. It then constructs the canonical execution URI from
`metadata.name` and requires exactly one result equal to `EXECUTION_URI`; zero,
multiple, malformed-label, cross-Job, or out-of-window results fail closed.

The reviewed collection shape has no result limit and lets gcloud exhaust all
pages in the exact narrow window; its selected projection is piped directly to
the reducer rather than saved as a provider payload:

```sh
gcloud run jobs executions list \
  --job="$SID_OPERATION_JOB_ID" \
  --project="$PROJECT" --region="$REGION" \
  --filter="metadata.creationTimestamp >= '$T0_UTC' AND metadata.creationTimestamp < '$T1_UTC'" \
  --format='json(metadata.name,metadata.creationTimestamp,metadata.labels,status.completionTime,status.conditions)' \
| "$SIGNED_EXECUTION_DISCOVERY_REDUCER" compare \
    --project="$PROJECT" \
    --region="$REGION" \
    --expected-job-id="$SID_OPERATION_JOB_ID" \
    --expected-parent-job-uri="$SID_OPERATION_JOB_URI" \
    --expected-execution-uri="$EXECUTION_URI" \
    --start-inclusive="$T0_UTC" --end-exclusive="$T1_UTC"
```

The reducer exits nonzero on empty, duplicate, malformed, cross-Job, parent-
ambiguous, truncated, or window-escaping input. `JOB_BEFORE` UID/generation may
be supplied as signed context, but the reducer must never report that they came
from an Execution row. Its path and artifact digest are signed alongside the
004 reducer.

After terminal execution, describe the source Job again as `JOB_AFTER`. Its
full URI, UID, generation, immutable-template projection, and projection hash
must equal `JOB_BEFORE`. Separately, the execution's exact
`run.googleapis.com/job` label and the reducer-derived parent URI must identify
the signed source Job, and its description must prove that its immutable copied
task template equals `JOB_BEFORE`: one task, zero retries, exact args, exact
VPC/service account, immutable image, endpoint hash, and numeric secret version.
Record the execution's own identity and successful terminal condition
separately; never equate execution metadata with the source Job's UID/generation.
Retain only those bounded projections/hashes and the bounded SQL report.

Every `004_verify.sql` report must be consumed by the signed, checked-in,
deterministically tested canonical comparator before transition. Its executable
contract is:

```sh
"$SIGNED_VERIFY_REDUCER" compare \
  --input="$BOUNDED_004_REPORT" \
  --expected-sid-count=19 \
  --expected-feed-count=154 \
  --expected-manifest-digest="$REVIEWED_MANIFEST_DIGEST"
```

The reducer must require exactly one well-formed manifest row, lowercase 64-hex
digest, no duplicate/trailing candidate, exact 19/154 values, and byte-for-byte
digest equality. It emits one canonical bounded result and exits nonzero on any
mismatch or malformed/truncated input. Visual inspection, `grep`, SQL's internal
19/154 assertion alone, or merely retaining the emitted digest does not satisfy
the transition gate. The reducer path and artifact SHA-256 are part of the
signed manifest; current deployment lacks this reducer, which is another reason
Gate 0 remains blocked.

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
3. **pre-schema:** execute the exact controlled **recovery-safe** schema-migration
   generation and ordered migrations manifest that excludes or safely guards
   migration 019 while pg_cron is off. Prove the manifest/terminal result, then
   execute checked `001_preseed.sql`; if SID authority is intended, execute
   checked `002_activate.sql`; finally run `004_verify.sql`. Migration 019 remains
   deferred to final activation. A pre-schema restore never skips directly to
   claims, and the current all-in-one schema runner cannot satisfy this branch.

The current schema-migration template is one task with `max_retries=1`; a single
provider execution can therefore contain at most two attempts. That bounded
provider retry is part of the signed execution. It is not permission to launch a
second execution. A terminal schema-migration failure can leave an idempotent
prefix applied and must be classified by bounded catalog/004 verification.

Every branch, including a branch that remains dormant, must finish with
`004_verify.sql` reporting exactly **19 SIDs**, **154 Feeds**, and the exact
reviewed manifest digest from the signed incident manifest **and the canonical
comparator exiting zero**. The 19/154 values are not adjustable incident inputs.
A different restored membership is a stop condition requiring a new recovery
decision, not an edited SQL argument.

Across all branches, **never mass-reset** owner, status, failure count/reason,
membership revision, cursor/progress, Lease identity, or fence. Preserve durable
history and use only the committed operations.

Before leaving this state, the reviewed internal-fence operation must make the
restored timeline safe for a later engine restart: activated/dormant restores
durably disable the exact two restored cron schedules while the engine is off;
pre-schema restores prove the cron schema/Jobs absent. Re-run the bounded
internal inventory and prove no new/nonterminal run. The normal definitions are
restored only at the end of `RESTORED_ACTIVE`.

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
  and a signed `T1_UTC`, list bounded executions from the exact Job and use the
  same canonical URI/discovery reducer over `[T0_UTC, T1_UTC]`. Exactly one
  candidate whose exact `run.googleapis.com/job` label equals the validated Job
  ID and whose reducer-derived parent URI equals the signed full Job URI may be
  described and followed to terminal state. Zero, multiple, malformed-label,
  or cross-Job candidates remain unknown and stop the runbook. Job
  UID/generation continuity is established only by the separate
  `JOB_BEFORE`/`JOB_AFTER` descriptions.
- After any unknown mutating outcome, run the read-only `004_verify.sql` surface
  only after the unique execution is terminal or provider cancellation is
  terminal. Compare complete expected state. Never use a blind retry to discover
  whether activation or migration was consumed.

**Expected bounded output:** One classified branch, exact terminal execution
URIs, canonical comparator success at 19/154 with the signed digest, intended
explicit migration/pre-seed/activation effects only, preserved history, inert
internal automation, and zero claims.

**Evidence:** Branch decision, frozen SQL object generations/hashes, Job bounded
projection, exact execution URIs, terminal results, before/after bounded reports,
final manifest report/hash, reviewer and UTC.
The evidence includes the comparator artifact hash, canonical result, exit-zero
record, pre/post source-Job UID/generation hashes, canonical execution URI, and
post-T0 discovery result.

**Stop condition:** Ambiguous schema/branch, digest mismatch, unexplained durable
delta, dormant stale recovery, wrong endpoint/secret/Job generation, failed
assertion, comparator nonzero, source-Job pre/post mismatch, noncanonical or
ambiguous execution, pg_cron activity, nonterminal execution, or unknown outcome.
A last activation assertion failure is not permission to retry.

**Abort/fallback:** Keep both timelines externally fenced and investigate with
bounded read-only reports. Do not repair uncertainty with broad updates.

### RESTORED_ACTIVE

**Entry gate:** Reprove complete old-timeline and restored maintenance absence.
`RESTORED_ENDPOINT_READY` remains eight-of-eight. `RESTORED_DATA_CLASSIFIED`
passes its final 004 report and canonical comparison. Both internal ledgers are
reproved: source engine off, restored engine off, and the restored schedules
disabled/absent. Endpoint, credentials, authority mode, profile, image, and
replacement templates agree. No deploy, MIG action, trigger delivery, internal
run, or Cloud Run Job execution is queued.

**Mutation:** Move out of maintenance in explicit groups, re-attesting the
restored endpoint, source absence, and internal-automation state between groups:

1. while the exact restored cron schedules remain disabled/absent, enable the
   restored pg_cron engine through the signed IaC safety-delta reversal, wait
   through the instance restart, and prove no cron run was created;
2. enable only `audio_segments_api`, `feed_store`, and `rules_management` on
   their already-proved restored revisions;
3. start the collector exactly once on the same reviewed slots, observe ordinary
   claims with fencing tokens newer than restored durable state, and run the
   exact 19/19 bootstrap plus post-bootstrap 30-minute soak gates from the SID
   cutover runbook;
4. after acceptance, enable Echo service/Eventarc and the Publisher
   service/Scheduler on their restored revisions; and
5. restore normal Cloud Run Job invocation, autoscaler, autohealing, repair,
   boot, restart,
   and deployment controls only after proving no queued execution/deploy and
   re-attesting the complete live topology; then
6. last, execute the reviewed exact migration-019/internal-activation operation
   to create/update and enable only `feeds-abandoned-lease-sweep` and `feeds-vac`.
   Compare their names, schedules, database/user, command digests, and first run
   status to the signed manifest, then run 004 plus the canonical comparator
   once more.

`bcfy_calls_sid_operation` returns to its read-only default and
`schema_migration` to its reviewed normal trigger contract; neither is executed
merely to prove it is enabled. The source engine and schedules remain disabled.
If any group fails, disable restored cron first if it has reached step 6, then
re-fence the already-enabled restored groups before evaluating fallback.

**Expected bounded output:** One live database timeline; monotonic fences; exact
19/19 bootstrap; passing soak; all eight intended consumers on restored; source
and old fleet still externally fenced; exact internal Jobs active only on
restored; final 004 comparator success.

**Evidence:** Startup/fence rows, bootstrap/soak reducer-safe projected-event
population object IDs and hashes, reducer results, all-eight topology proof,
control-restoration report, and signed
acceptance decision, plus both internal ledgers and final cron/004 proofs.

**Stop condition:** Any old process, stale endpoint, non-monotonic fence,
bootstrap/soak failure, incomplete consumer, credential error, unexpected Job,
unexpected cron definition/run, source pg_cron activity, comparator failure, or
durable/runtime divergence. Stop restored authority before fallback; never run
both timelines to compare them.

**Abort/fallback:** Re-enter complete no-authority absence for all eight
consumers. While both timelines are fenced, select source only through a reviewed
all-eight endpoint rollback plan, revalidate source credentials/data, enable the
source engine only while its exact schedules remain disabled, start one timeline
once, and restore the exact cron Jobs last.

### FALLBACK_HOLD_COMPLETE

**Entry gate:** `RESTORED_ACTIVE` passes its approved observation period and the
incident commander accepts the restored timeline.

**Mutation:** Retain the source cluster and old fleet fenced for the approved
fallback period with its pg_cron engine and exact schedules disabled. Reprove
that internal hold throughout retention. At expiration, use the organization's
separate retention and resource-lifecycle process. This runbook does not
authorize source cleanup.

**Expected bounded output:** One active restored timeline, one fenced retained
source, normal controls restored only on active topology, and an accountable
fallback-expiry decision. No source internal run occurs during the hold.

**Evidence:** Final topology hashes, external and internal control states/run
frontiers, retention owner/deadline, operator/reviewer/UTC.

**Stop condition:** Consumer drift, unowned fallback deadline, missing evidence,
or any HIGH finding keeps the fallback fence in place.

**Abort/fallback:** Extend fenced retention under incident ownership.

## Evidence index

Append a PITR section to the cutover evidence index containing only:

- incident/change ID, official-document review timestamp, bounded SDK version
  hash, and signed manifest hash;
- source/restored full resource URIs, UIDs, project/region/network/PSA/KMS
  bindings, durable IaC addresses, and bounded description hashes;
- target UTC; automated/continuous backup, deletion-protection, labels, Query
  Insights, pooling and signed flag-delta proofs;
- all-eight durable fence/convergence ledger and exact provider operation IDs;
- separate source/restored internal-automation ledgers, exact cron definitions/
  command digests/run frontiers, and engine flag operation IDs;
- restore/create operation IDs and topology equality/delta matrix;
- numeric Secret Manager version resource IDs and direct/pooled query hashes;
- immutable MIG worker-secret binding, never a password value/hash or user data;
- frozen SQL/reducer generation/digest manifest, source-Job pre/post UID/
  generation hashes, canonical execution URIs, bounded post-T0 discovery,
  branch, and final comparator-accepted 19/154 manifest digest;
- startup/fence, bootstrap, soak, control-restoration, and fallback-hold evidence;
  and
- operator, reviewer, commander, UTC, decisions, and exceptions.

Do not retain private endpoint values, password material, full environments,
signed URLs, bearer tokens, connection strings, or unbounded provider payloads.
The evidence index must retain the literal status `PITR REVIEWED — NOT
EXERCISED` until a separately approved live restore, endpoint switch, data
classification, start, soak, fallback, and complete external-fence exercise
plus internal-writer exercise actually passes.

## Tabletop failure matrix

Tabletop validates fail-closed branches; it does not replace Gate 0
implementation or a non-production rehearsal.

| Injection | Required response | Opposite/restored authority |
|---|---|---|
| Gate 0 consumer lacks durable fence | abort in `DR_PREPARED`; no restore/endpoint/SQL | stopped |
| collector process stopped but template still embeds raw password | reject MIG row and endpoint plan; no restore | stopped |
| source cron Job remains scheduled/running | reject internal ledger before restore | stopped |
| restored primary created with pg_cron enabled | isolate it; no endpoint/probe/SQL | stopped |
| migration 019 appears in pre-schema hold manifest | reject schema execution; require recovery-safe manifest | stopped |
| source pg_cron runs during fallback hold | reject retention state and investigate source mutation | stopped |
| Cloud Run service scales to zero but remains invocable | reject fence row; scaling is not absence | stopped |
| Eventarc trigger deleted without reviewed recreation/drain | reject fence row | stopped |
| Scheduler paused but Publisher service remains invocable | reject fence row | stopped |
| pending/running Job execution | cancel exact execution, wait terminal, reprove; no new execution | stopped |
| Job or consumer resolves secret `latest` | abort before endpoint/SQL | stopped |
| wrong restored credential or SQL identity | stop in `RESTORED_ENDPOINT_READY` | stopped |
| endpoint fan-out miss | reject readiness and retain all fences | stopped |
| backup/deletion-protection/label/Query-Insights/IaC parity miss | reject restored readiness | stopped |
| failed durable Terraform apply | retain fences; review source rollback or corrected plan | stopped |
| pre-schema restore | recovery-safe migrations excluding/guarding 019, pre-seed/activation as intended, then canonical 004 comparison | stopped |
| dormant-row restore | 004 proves dormancy; checked activation only after explicit intent | stopped |
| 004 emits a digest but comparator is absent/nonzero | reject data-classified transition | stopped |
| execution short name cannot bind to canonical Job-qualified URI | outcome unknown; no retry | stopped |
| source Job UID/generation changes before/after execution | reject execution evidence and stop | stopped |
| returned activation assertion rollback | record known failure; investigate; no automatic retry | stopped |
| transport loss/unknown Job outcome | discover unique post-T0 execution, follow terminal, read-only verify; no blind retry | stopped |
| network/PSA/KMS mismatch | isolate restored resource; no endpoint/SQL | stopped |
| unavailable old VM or unknown consumer | treat as ambiguity, never absence | stopped |

### Independent review 1 — external authority and topology

Reviewer verifies Gate 0 implementation, all-eight suppression/drain/absence,
complete trigger/invoker/execution inventory, internal pg_cron suppression/drain,
restart/redeploy suppression, resource binding, backup/deletion/IaC parity,
endpoint-before-SQL ordering, one-timeline start, and fallback hold.
Any HIGH finding blocks execution.

**Reviewer:** ____  **UTC:** ____  **Findings/resolutions:** ____

### Independent review 2 — data, Job, and credential integrity

Reviewer verifies backup bounds, network/PSA/KMS/config equality, direct 5432 and
pooled 6432 identity probes, numeric secret pins/MIG binding, exact source Job
pre/post generations and canonical execution URIs, SQL/reducer generations/
hashes, activated/dormant/pre-schema branches, recovery-safe migration 019
ordering, final comparator-accepted 004 19/154/digest proof, unknown-outcome
handling, and secret-safe evidence.

**Reviewer:** ____  **UTC:** ____  **Findings/resolutions:** ____

Both independent reviews must conclude **no unmitigated HIGH** finding before a
future execution may proceed.

## Publication assertions

This reviewed procedure intentionally makes no live-recovery claim:

`PITR REVIEWED — NOT EXERCISED`

`NO PRODUCTION CUTOVER PERFORMED`

`LIVE PITR BLOCKED UNTIL GATE 0 IS IMPLEMENTED AND REHEARSED`
