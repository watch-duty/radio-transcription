---
phase: 02-cloud-logging-and-pub-sub-routing
plan: 03
subsystem: infra
tags: [terraform, app-module, cloud-logging, pubsub, validation]
requires:
  - phase: 02-cloud-logging-and-pub-sub-routing
    provides: Plan 02-01 message queue outputs and Plan 02-02 route module
provides:
  - App-module wiring for the Feed Audit Notification route
  - Disabled-by-default route enablement and relay input contract
  - Nullable app outputs for sink, subscription, push endpoint, and push invoker identity
  - Dev and prod Terraform validation for the complete Phase 2 route shape
affects: [phase-03, deployment]
tech-stack:
  added: []
  patterns: [thin environment roots, disabled-by-default app submodule wiring, nullable verification outputs]
key-files:
  created: []
  modified:
    - radio-transcription-deployment/terraform/modules/app/main.tf
    - radio-transcription-deployment/terraform/modules/app/variables.tf
    - radio-transcription-deployment/terraform/modules/app/outputs.tf
key-decisions:
  - "Wire the route module through terraform/modules/app rather than duplicating resources in dev/prod roots."
  - "Keep feed_audit_notification_route_enabled default false until Phase 3 supplies a real relay service URL/name."
  - "Expose route outputs as nullable app outputs so validation can prove wiring without requiring route enablement."
patterns-established:
  - "App module composes optional route modules with count and null-safe outputs."
  - "Phase 2 infrastructure validation includes both dev and prod roots before route enablement."
requirements-completed:
  - ROUTE-01
  - ROUTE-02
  - ROUTE-03
  - ROUTE-04
duration: 8min
completed: 2026-06-27
---

# Phase 02 Plan 03: App Module Route Wiring Summary

**Disabled-by-default app-module composition for Feed Audit Notification Cloud Logging to Pub/Sub routing**

## Performance

- **Duration:** 8 min
- **Started:** 2026-06-27T00:43:26Z
- **Completed:** 2026-06-27T00:51:14Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

- Added app-level variables for enabling the route and passing real relay URL/name inputs.
- Wired `module "feed_audit_notification_route"` into `terraform/modules/app` using Plan 02-01 topic/DLQ outputs and `local.deployer_sa_email`.
- Added nullable app outputs for sink name, subscription name, push endpoint, and push invoker service account email.
- Validated the complete Terraform graph for both dev and prod with `terraform init -backend=false` and `terraform validate`.

## Task Commits

Each task was committed atomically in the deployment repository branch `gsd/phase-02-cloud-logging-and-pub-sub-routing`:

1. **Task 1: Add app-level relay input contract and route module call** - `eae258e` (`feat(02-03): wire feed audit route into app module`)
2. **Task 2: Expose route outputs and run focused static validation** - `951fe4d` (`feat(02-03): expose feed audit route outputs`)

## Files Created/Modified

- `radio-transcription-deployment/terraform/modules/app/main.tf` - Adds disabled-by-default route module composition.
- `radio-transcription-deployment/terraform/modules/app/variables.tf` - Adds route enablement and relay input variables.
- `radio-transcription-deployment/terraform/modules/app/outputs.tf` - Adds null-safe route verification outputs.

## Decisions Made

- Left dev/prod environment roots unchanged; app module owns the route composition.
- Kept the route disabled by default so this phase can validate before the Phase 3 relay service exists.
- Passed `deployer_service_account_email = local.deployer_sa_email` to keep the ActAs grant tied to the existing WIF deployer identity.

## Deviations from Plan

None - plan executed exactly as written.

---

**Total deviations:** 0 auto-fixed.
**Impact on plan:** No scope changes.

## Issues Encountered

- Terraform formatting aligns `count  =` with two spaces, so the exact human-readable acceptance string was verified using a whitespace-tolerant regex while preserving `terraform fmt`.

## User Setup Required

None - no external service configuration required.

## Verification

- `safe-run -- terraform fmt -check terraform/modules/message_queues terraform/modules/feed_audit_notification_route terraform/modules/app`
- `safe-run -- terraform -chdir=terraform/environments/dev init -backend=false`
- `safe-run -- terraform -chdir=terraform/environments/dev validate`
- `safe-run -- terraform -chdir=terraform/environments/prod init -backend=false`
- `safe-run -- terraform -chdir=terraform/environments/prod validate`
- `rg -n 'feed_audit_notification_logging_sink_name|feed_audit_notification_subscription_name|feed_audit_notification_push_endpoint|feed_audit_notification_push_invoker_service_account_email|jsonPayload\.event_type="radio_transcription\.feed_audit_notification"|jsonPayload\.schema_version=1|roles/pubsub.publisher|audience\s+= var\.relay_service_url|minimum_backoff = "10s"|maximum_backoff = "60s"|max_delivery_attempts = 10' terraform/modules`
- `rg -n 'resource "google_cloud_run_v2_service"|container/hello|wd_backend_endpoint_api_key|X-Api-Key|requests|httpx' terraform/modules/feed_audit_notification_route` returned no matches.
- `rg -n 'google_cloud_run_v2_service" "feed_audit|feed-audit-notification-relay' terraform/modules/app` returned no matches.

## Next Phase Readiness

Phase 3 can now add the Cloud Run relay service and enable the route by passing real relay service URL/name values into the app module.

---

*Phase: 02-cloud-logging-and-pub-sub-routing*
*Completed: 2026-06-27*
