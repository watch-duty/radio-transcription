---
phase: 02-cloud-logging-and-pub-sub-routing
plan: 02
subsystem: infra
tags: [terraform, cloud-logging, pubsub, cloud-run, iam, dlq]
requires:
  - phase: 02-cloud-logging-and-pub-sub-routing
    provides: Plan 02-01 notification topic and DLQ outputs
provides:
  - Feed Audit Notification route Terraform module
  - Event-contract-only Cloud Logging sink and topic-scoped sink publisher IAM
  - Authenticated Pub/Sub push subscription contract for the Phase 3 relay
  - Dedicated Pub/Sub push invoker service account and least-privilege IAM grants
  - Retry and dead-letter policy for routed audit notifications
affects: [phase-02, phase-03, deployment]
tech-stack:
  added: []
  patterns: [cloud logging sink to pubsub, pubsub oidc push, service-account-scoped iam]
key-files:
  created:
    - radio-transcription-deployment/terraform/modules/feed_audit_notification_route/main.tf
    - radio-transcription-deployment/terraform/modules/feed_audit_notification_route/variables.tf
    - radio-transcription-deployment/terraform/modules/feed_audit_notification_route/outputs.tf
    - radio-transcription-deployment/terraform/modules/feed_audit_notification_route/versions.tf
  modified: []
key-decisions:
  - "Use only the Feed Audit Notification event contract in the sink filter so future emitters are not accidentally dropped."
  - "Grant the Terraform deployer roles/iam.serviceAccountUser only on the dedicated push invoker service account."
  - "Set Pub/Sub OIDC audience to the relay service URL while the push endpoint appends the relay path."
patterns-established:
  - "Cloud Logging sink writer receives topic-level Pub/Sub Publisher instead of project-level Pub/Sub Publisher."
  - "Pub/Sub authenticated push uses a route-dedicated service account plus Cloud Run service-level invoker IAM."
requirements-completed:
  - ROUTE-01
  - ROUTE-02
  - ROUTE-03
  - ROUTE-04
duration: 10min
completed: 2026-06-27
---

# Phase 02 Plan 02: Feed Audit Notification Route Module Summary

**Terraform route module for event-contract Cloud Logging fanout into authenticated Pub/Sub push delivery**

## Performance

- **Duration:** 10 min
- **Started:** 2026-06-27T00:42:48Z
- **Completed:** 2026-06-27T00:52:32Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- Added a reusable `feed_audit_notification_route` Terraform module in the deployment repository.
- Added a Cloud Logging project sink that routes only `radio_transcription.feed_audit_notification` schema v1 log entries.
- Granted the sink writer `roles/pubsub.publisher` only on the notification topic.
- Added authenticated Pub/Sub push delivery to the future relay path with explicit Cloud Run service URL audience.
- Added retry, dead-letter, Pub/Sub service-agent IAM, and deployer ActAs grants.

## Task Commits

Each task was committed atomically in the deployment repository branch `gsd/phase-02-cloud-logging-and-pub-sub-routing`:

1. **Task 1: Define route module interface and outputs** - `d60978b` (`feat(02-02): add feed audit route module interface`)
2. **Task 2: Add Cloud Logging sink and topic-scoped publisher IAM** - `4d775cf` (`feat(02-02): add feed audit log sink route`)
3. **Task 3: Add authenticated push subscription, retry, and DLQ IAM** - `6930203` (`feat(02-02): add feed audit notification push route`)

## Files Created/Modified

- `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/variables.tf` - Defines topic, DLQ, relay, and deployer identity inputs.
- `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/outputs.tf` - Exposes sink, subscription, endpoint, and push invoker outputs.
- `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/versions.tf` - Pins Terraform and Google provider constraints to match deployment modules.
- `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/main.tf` - Defines the sink, IAM, push subscription, retry, and DLQ route resources.

## Decisions Made

- Kept the sink filter to exactly `jsonPayload.event_type="radio_transcription.feed_audit_notification"` and `AND jsonPayload.schema_version=1`.
- Used `google_service_account_iam_member` for the Pub/Sub service agent token-creator grant so token creation is scoped to the route push invoker account.
- Added a deployer ActAs grant on the push invoker account so authenticated push subscription creation can apply cleanly through Terraform.

## Deviations from Plan

None - plan executed exactly as written.

---

**Total deviations:** 0 auto-fixed.
**Impact on plan:** No scope changes.

## Issues Encountered

- The broad negative inspection guard matched the word `requests` in a comment. The comment was reworded before the Task 3 commit so the guard remains unambiguous.

## User Setup Required

None - no external service configuration required.

## Verification

- `safe-run -- terraform fmt -check terraform/modules/feed_audit_notification_route`
- `rg -n 'jsonPayload\.event_type="radio_transcription\.feed_audit_notification"|jsonPayload\.schema_version=1|roles/pubsub.publisher|roles/iam.serviceAccountUser|minimum_backoff = "10s"|maximum_backoff = "60s"|max_delivery_attempts = 10|audience\s+= var\.relay_service_url|roles/run.invoker' terraform/modules/feed_audit_notification_route`
- `rg -n 'resource "google_cloud_run_v2_service"|container/hello|wd_backend_endpoint_api_key|X-Api-Key|requests|httpx' terraform/modules/feed_audit_notification_route` returned no matches after the comment cleanup.

## Next Phase Readiness

The route module is ready for Plan 02-03 to wire it into `terraform/modules/app` behind a disabled-by-default enable flag and real relay URL/name inputs.

---

*Phase: 02-cloud-logging-and-pub-sub-routing*
*Completed: 2026-06-27*
