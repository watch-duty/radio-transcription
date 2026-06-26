# Phase 2: Cloud Logging and Pub/Sub Routing - Context

**Gathered:** 2026-06-26
**Status:** Ready for planning

## Phase Boundary

Phase 2 creates the infrastructure route that moves Phase 1 Feed Audit
Notification structured logs from Cloud Logging to a dedicated Pub/Sub push
delivery path. It owns the Log Router sink, notification topic, DLQ topic,
minimal sink publisher IAM, push subscription shape, authenticated Pub/Sub
push identity, retry policy, and dead-letter policy.

Phase 2 does not implement the relay application, parse Pub/Sub messages,
call the Watch Duty webhook, add producer code, add database polling, or add
delivery state tables. Phase 3 supplies the relay Cloud Run service and wires
its outputs into the route.

## Implementation Decisions

### Terraform Ownership
- **D-01:** Reusable, parameterized Terraform modules or contracts should live
  in the public `radio-transcription` repo when they are genuinely reusable.
  Environment-specific instantiation and values belong in
  `radio-transcription-deployment`.
- **D-02:** If the route cannot be made reusable without encoding deployment
  assumptions, the public repo should document only the contract and the
  deployment repo should own the concrete resources.
- **D-03:** Based on the latest deployment repo shape, planners should prefer a
  small route module or app submodule called from `terraform/modules/app`,
  because dev and prod environment roots are intentionally thin wrappers.

### Log Sink Filter
- **D-04:** The Log Router sink filter should be based on the event contract
  only:
  `jsonPayload.event_type="radio_transcription.feed_audit_notification"` and
  `jsonPayload.schema_version=1`.
- **D-05:** Do not add Cloud Run service names, resource types, runtime names,
  or environment-specific emitter filters to the sink unless research proves
  they are required for correctness. The event contract is the routing
  boundary; extra filters increase maintenance cost and risk missing future
  valid emitters.

### Delivery Route Scope
- **D-06:** Phase 2 should create the full delivery route shape: dedicated
  notification Pub/Sub topic, DLQ topic, Log Router sink, sink writer IAM,
  Pub/Sub push subscription, OIDC-authenticated push config, Pub/Sub service
  agent IAM for token creation and DLQ handling, retry backoff, and
  dead-letter policy.
- **D-07:** The route should be parameterized with relay inputs rather than
  creating a placeholder relay. The intended inputs are the relay service URL,
  relay service name, and the push invoker service account or enough data to
  grant `roles/run.invoker`.
- **D-08:** The route should use 10 second minimum backoff, 60 second maximum
  backoff, and a dead-letter policy capped at 10 delivery attempts, matching
  Phase 2 requirements.

### IAM Shape
- **D-09:** Use a dedicated Pub/Sub push invoker service account for the feed
  audit notification route, matching the existing service-module push
  subscription pattern. Do not reuse an application runtime service account for
  Pub/Sub push authentication.
- **D-10:** Grant the Log Router sink writer only the Pub/Sub Publisher role
  on the notification topic. Do not grant project-wide publisher permissions
  unless Terraform provider/resource constraints force it.
- **D-11:** Grant the Pub/Sub service agent only the IAM required for push OIDC
  token creation and dead-letter behavior, following the existing deployment
  repo pattern.

### the agent's Discretion

The planner may choose the exact module name and variable/output names, but
should keep names aligned with existing deployment conventions. Prefer names
that make the domain obvious, such as `feed_audit_notification` or
`feed_audit_notification_route`, rather than generic webhook or relay names
inside Phase 2.

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project And Requirements
- `.planning/PROJECT.md` - Defines the milestone, critical-path constraint,
  WD webhook destination, and Cloud Logging to Pub/Sub direction.
- `.planning/REQUIREMENTS.md` - Lists Phase 2 requirements `ROUTE-01..04` and
  adjacent Phase 3 relay requirements.
- `.planning/ROADMAP.md` - Defines Phase 2 success criteria and boundaries.
- `.planning/STATE.md` - Captures accumulated decisions from Phase 1 and the
  current Phase 2 focus.
- `.planning/phases/01-audit-contract-and-emission/01-CONTEXT.md` - Locks the
  producer-side event contract, event type, schema version, and non-critical
  path constraints.
- `.planning/phases/01-audit-contract-and-emission/01-VERIFICATION.md` - Shows
  Phase 1 completion status and verification scope.

### Public Repo Producer Contract
- `backend/pipeline/storage/feed_audit_notifications.py` - Shared producer
  helper that emits structured notification logs.
- `backend/pipeline/common/log_helper.py` - Existing structured logging pattern
  using `extra={"json_fields": ...}`.
- `backend/pipeline/storage/feed_queries.py` - Async audited SQL that now
  returns `feed_audit_event` payloads when audit rows are inserted.
- `backend/pipeline/storage/sync_feed_queries.py` - Sync audited SQL that now
  returns the same notification payload shape.

### Deployment Repo Patterns
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/modules/app/main.tf` - App composition module; dev/prod env roots call this module.
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/modules/message_queues/main.tf` - Central Pub/Sub topic and DLQ patterns.
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/modules/message_queues/outputs.tf` - Topic/DLQ output naming patterns.
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/modules/services/notification/main.tf` - Closest existing push subscription example with OIDC, retry policy, DLQ policy, and Pub/Sub service-agent IAM.
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/modules/services/transcription/main.tf` - Dedicated Pub/Sub push invoker service account and Cloud Run invoker IAM pattern.
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/modules/services/normalization/main.tf` - Push subscription, DLQ publisher IAM, and token creator pattern.
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/environments/dev/main.tf` - Thin environment wrapper around `modules/app`.
- `watch-duty/radio-transcription-deployment@14ac7c4:terraform/environments/prod/main.tf` - Thin prod wrapper plus monitoring channel root resource.
- `watch-duty/radio-transcription-deployment@14ac7c4:.github/workflows/terraform-lint.yml` - Terraform validation path for PRs.
- `watch-duty/radio-transcription-deployment@14ac7c4:.github/workflows/terraform_deploy.yml` - Deploy workflow and public-source env-var contract check.

### Official Platform Docs
- `https://docs.cloud.google.com/logging/docs/export/pubsub` - Cloud Logging
  sink to Pub/Sub behavior, Pub/Sub message shape, and sink writer IAM.
- `https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions` - Pub/Sub `pushConfig`, `retryPolicy`, and `deadLetterPolicy`
  fields.

## Existing Code Insights

### Reusable Assets
- The public repo already owns the emitted structured log contract. Phase 2
  should consume only the `event_type` and `schema_version` fields for routing.
- The deployment repo already centralizes Pub/Sub topics and DLQs in
  `terraform/modules/message_queues`.
- The deployment repo already defines per-service Pub/Sub push subscriptions
  with OIDC, Cloud Run invoker IAM, DLQ policy, and Pub/Sub service-agent IAM.

### Established Patterns
- Dev and prod environments call `module "app"` and pass variables down. Avoid
  duplicating the route in both environment roots unless an environment-only
  secret or value forces it.
- Existing service modules usually create a service account for the runtime
  and a separate service account for Pub/Sub push invocation.
- Existing Pub/Sub DLQs have retention subscriptions so dead-lettered messages
  do not disappear immediately.
- Existing CI runs `terraform init -backend=false`, `mise run flatten-schemas`,
  `terraform validate`, and `mise run check` for Terraform PRs.

### Integration Points
- Add topic/DLQ outputs if the route needs new topics from the message queue
  module.
- Add or call a route module from `terraform/modules/app/main.tf`.
- Add variables and outputs in `terraform/modules/app` only for values Phase 3
  needs to provide or consume.
- The future Phase 3 relay should expose service URL/name and identity data
  needed by Phase 2 route wiring.

## Specific Ideas

Prefer a Terraform shape that separates route mechanics from relay
implementation:

```hcl
module "feed_audit_notification_route" {
  source = "../feed_audit_notification_route"

  project_id          = var.project_id
  environment         = var.environment
  notification_topic  = module.message_queues.feed_audit_notification_topic_id
  dead_letter_topic   = module.message_queues.feed_audit_notification_dlq_id
  relay_service_url   = module.feed_audit_webhook_relay.service_url
  relay_service_name  = module.feed_audit_webhook_relay.service_name
}
```

The exact names are discretionary. The important part is that Phase 2 can be
planned as route infrastructure with relay outputs as an explicit dependency
contract, not as a placeholder Cloud Run service.

## Deferred Ideas

- Implementing the Cloud Run relay application belongs to Phase 3.
- Parsing Pub/Sub `LogEntry` envelopes, extracting `jsonPayload`, and calling
  the Watch Duty webhook belongs to Phase 3.
- Operational dashboards, route proof, DLQ runbooks, and staging/prod rollout
  verification belong to Phase 4.
- Any future replay API, delivery-status UI, or multi-destination fanout remains
  v2 scope.

---

*Phase: 2-Cloud Logging and Pub/Sub Routing*
*Context gathered: 2026-06-26*
