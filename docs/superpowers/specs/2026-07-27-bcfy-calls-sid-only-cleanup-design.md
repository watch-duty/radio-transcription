# Broadcastify Calls SID-Only Cleanup Design

**Status:** Approved on 2026-07-27 after successful dev and production soak.

## Context

Broadcastify Calls ingestion has completed its authority cutover in both
environments. Dev and production run with `BCFY_CALLS_AUTHORITY_MODE=sid_lease`
and `CAP_BCFY_CALLS=0`; durable parent SID leases are the only active authority,
and no eligible child Feed retains legacy ownership.

The application still contains the temporary process-local authority switch and
legacy Calls Feed-cap configuration used to stage that cutover. The deployment
repository still supplies both values explicitly. Keeping these controls after
successful soak preserves a configuration path that can create mixed Feed and
SID authority during a future rollout.

## Goals

- Make SID leases the unconditional authority for Broadcastify Calls.
- Make it impossible to re-enable legacy Calls Feed claims through environment
  or Terraform configuration.
- Preserve Feed-domain ingestion for Broadcastify streams, OpenMHz, and Fire
  Notifications.
- Preserve the existing `/healthz` authority field as stable operational
  evidence with the constant value `sid_lease`.
- Remove stale cutover configuration and comments from the deployment
  repository.
- Deliver the cleanup as two ordered, independently reviewable pull requests.

## Non-Goals

- No GOO-768 admin mutation, read-model, or Reporter UI behavior.
- No database schema or durable lease-row deletion.
- No change to SID admission capacity, work concurrency, failure policy,
  fencing, heartbeat, or page-settlement behavior.
- No merge of the completed cutover SQL branch. Its execution evidence is
  already recorded on GOO-768, and its legacy rollback procedure becomes stale
  after this cleanup.
- No generic removal of Feed-domain profiles used by other sources and
  supervisor tests.

## Design

### Pull Request 1: Application authority cleanup

Repository: `watch-duty/radio-transcription`

The collector will construct one mixed Feed-and-SID worker profile. Feed-domain
claims remain enabled for non-Calls sources, and SID-domain claims remain
enabled for Calls. The runtime will no longer parse or branch on
`BCFY_CALLS_AUTHORITY_MODE`.

The settings layer will:

- remove the Calls authority enum, environment loader, settings field, and
  authority-derivation function;
- construct an enabled mixed worker profile directly from the configured Feed
  and SID capacities;
- exclude `SourceType.BCFY_CALLS` from Feed claim caps unconditionally; and
- stop reading `CAP_BCFY_CALLS`, while continuing to load caps for the remaining
  Feed-authority source types.

The source runtime registry will continue to describe Broadcastify Calls topic
routing and provider URL metadata. Feed-claim-cap construction will explicitly
distinguish the Calls SID path from sources leased through Feed rows, rather
than treating all VM-supported collectors as Feed-claimable.

The health response and startup telemetry will continue to expose
`bcfy_calls_authority_mode`, but its value will be the constant `sid_lease`.
This avoids an unnecessary operational API break while removing the
configuration path.

Focused tests will prove:

- an unset or conflicting legacy environment value cannot enable Calls Feed
  claims;
- the mixed profile enables both Feed and SID domains;
- Calls is absent from Feed claim caps while every other current Feed source
  retains its configured/default cap;
- collector composition always registers both domains; and
- health/startup telemetry reports `sid_lease`.

### Pull Request 2: Deployment configuration cleanup

Repository: `watch-duty/radio-transcription-deployment`

The Terraform module interface will remove `bcfy_calls_authority_mode` and
`bcfy_calls_cap` from both the application and ingestion modules. Collector
containers will no longer receive `BCFY_CALLS_AUTHORITY_MODE` or
`CAP_BCFY_CALLS`. Dev and production environment modules will remove their
explicit `sid_lease` and zero-cap assignments, along with stale cutover-stage
comments.

This PR depends on Pull Request 1 being merged, built, and deployed first. The
application cleanup safely ignores the still-present environment variables
during that intermediate rollout. Once the SID-only binary is present, removing
the variables cannot change authority behavior.

Terraform formatting and validation must pass for both environments. The plan
must show only removal of the obsolete environment entries and module inputs;
it must not change SID capacities, MIG size, container image selection, or
unrelated infrastructure.

## Delivery Sequence

1. Merge and deploy the application cleanup while deployment still supplies
   `sid_lease` and zero-cap values.
2. Verify dev and production health, SID lease heartbeats, zero legacy Calls
   owners, and continuing `chunk_ingested` events.
3. Merge and deploy the Terraform cleanup.
4. Repeat the same authority and ingestion verification.
5. Delete obsolete cutover branches and local worktrees.

The normal rollback after step 3 is to redeploy a prior known-good SID-capable
application and deployment revision. Restoring legacy Feed authority is no
longer an ordinary configuration rollback; it would require restoring the old
binary and repeating the fully drained authority transition.

## Non-PR Cleanup

After both pull requests merge and their deployments validate:

- delete remote branch `agent/bcfy-calls-prod-sid-cutover-sql`;
- remove its local branch and worktree;
- remove the local `agent/prod-sid-cutover-direct` branch and worktree (its
  remote branch is already deleted); and
- remove the two cleanup PR worktrees after merge.

Durable `ingestion_leases` rows, including deactivated rows, must remain.
Fencing history and generic Feed storage must also remain.

## Alternatives Rejected

### Change the default but retain the switch

This reduces accidental rollback risk but preserves the mixed-authority failure
mode and leaves dead configuration and test paths. It does not complete the
cleanup.

### Keep explicit SID configuration forever

This retains operational visibility but makes correctness depend on two
independent values remaining synchronized across every environment. Constant
health telemetry provides the useful visibility without retaining the hazard.

### Combine repositories into one rollout

The repositories cannot share one atomic pull request. Separating application
and deployment changes creates a safe compatibility window and an explicit
merge order.
