# Phase 4: Operations and Rollout Proof - Context

**Gathered:** 2026-06-27
**Status:** Ready for planning

## Phase Boundary

Phase 4 proves and documents the already-built Feed Audit Notification path:
producer structured log, Cloud Logging sink, Pub/Sub push subscription, relay
delivery, Watch Duty webhook response, retry behavior, and DLQ visibility.

This phase owns operational logs/metrics/alerts where useful, deployment and
runbook documentation, and staging verification that creates a real feed audit
row and observes the downstream delivery path. It does not redesign delivery,
change feed audit SQL, add database polling, add delivery tables, add replay
tooling, or couple webhook delivery to ingestion or feed lifecycle writes.

## Implementation Decisions

### Prior Decisions Carry Forward
- **D-01:** Do not revisit the delivery architecture in Phase 4. The path is
  structured producer log -> Cloud Logging sink -> Pub/Sub push -> Cloud Run
  relay -> Watch Duty webhook.
- **D-02:** `feed_audit_events` remains the only durable audit ledger. Phase 4
  must not add an outbox, delivery state table, database trigger,
  `LISTEN/NOTIFY`, CDC, or DB polling relay.
- **D-03:** The route remains contract-based:
  `event_type="radio_transcription.feed_audit_notification"` and
  `schema_version=1`.
- **D-04:** Verification must use a real feed audit row and observe the route
  externally. Do not add special write-path code or fake delivery hooks to prove
  the path.
- **D-05:** Operational proof should cover producer emission, sink routing,
  Pub/Sub push delivery, relay success, relay retryable failure, relay
  permanent/config failure, and DLQ inspection.

### Discussion Outcome
- **D-06:** The user selected no additional gray-area discussion for Phase 4.
  Downstream planners should use the locked prior decisions and implement the
  simplest maintainable operational proof that satisfies `OPS-01..04`.

### the agent's Discretion

The planner may choose exact log-based metric names, alert thresholds, runbook
file names, staging verification command shape, and whether the proof is a
manual runbook plus script or workflow-assisted. Keep durable/replay features
out of v1 and keep concrete environment names, secrets, IAM bindings, and
console links in the deployment repo unless a reusable contract belongs in the
public repo.

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project And Requirements
- `.planning/PROJECT.md` - Defines the milestone, critical-path constraint,
  WD webhook destination, best-effort notification framing, and out-of-scope
  delivery mechanisms.
- `.planning/REQUIREMENTS.md` - Lists Phase 4 requirements `OPS-01..04` and
  adjacent relay/routing requirements that Phase 4 must prove.
- `.planning/ROADMAP.md` - Defines Phase 4 success criteria and boundaries.
- `.planning/STATE.md` - Captures accumulated decisions from Phases 1 through
  3.
- `.planning/phases/01-audit-contract-and-emission/01-CONTEXT.md` - Locks the
  producer event contract and failure-isolated logging behavior.
- `.planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md` -
  Locks the Cloud Logging sink, Pub/Sub push, retry, IAM, and DLQ route shape.
- `.planning/phases/03-webhook-relay-delivery/03-CONTEXT.md` - Locks the relay
  service behavior, WD client retry policy, ACK/NACK semantics, and public vs.
  deployment repo split.

### Public Repo Notification Code
- `backend/pipeline/storage/feed_audit_notifications.py` - Shared producer
  helper that emits Feed Audit Notification structured logs and swallows local
  failures.
- `backend/pipeline/common/feed_audit_notification_contract.py` - Shared v1
  event type, schema version, and required payload-field contract.
- `backend/pipeline/feed_audit_webhook/main.py` - Relay FastAPI endpoint and
  Pub/Sub ACK/NACK behavior.
- `backend/pipeline/feed_audit_webhook/pubsub.py` - Pub/Sub push envelope and
  Cloud Logging `LogEntry` parser.
- `backend/pipeline/feed_audit_webhook/wd_client.py` - WD webhook client,
  retry behavior, and delivery success/failure logs.
- `backend/pipeline/feed_audit_webhook/README.md` - Public relay runtime
  contract and storage-boundary note.
- `backend/pipeline/feed_audit_webhook/tests/` - Existing unit coverage for
  relay parsing, forwarding, settings, retry, and no-DB coupling.

### Deployment Repo Operations Surface
- `../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf` - Log sink, Pub/Sub push subscription, OIDC push identity, retry policy, 60 second ACK deadline, and DLQ policy.
- `../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/outputs.tf` - Sink, subscription, push endpoint, and push invoker outputs useful for runbooks.
- `../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf` - Cloud Run relay service, runtime service account, WD API key secret, and runtime roles.
- `../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf` - App composition, existing alert policy patterns, log-based metric pattern, and feed audit route/service wiring.
- `../../feed-audit-notification-routing-deployment/terraform/modules/app/outputs.tf` - App-level outputs for the feed audit route and relay service.
- `../../feed-audit-notification-routing-deployment/terraform/modules/app/dashboards/system_health_overview.json.tftpl` - Existing dashboard template and Pub/Sub/DLQ visualization patterns.
- `../../feed-audit-notification-routing-deployment/.github/workflows/app_deploy.yml` - Service deploy workflow and `feed_audit_webhook` deployment path.
- `../../feed-audit-notification-routing-deployment/.github/workflows/terraform_deploy.yml` - Terraform deploy workflow, WD env/secret inputs, and module path filters.
- `../../feed-audit-notification-routing-deployment/README.md` - Existing deployment and dashboard export workflow.
- `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/console-deep-links.md` - Existing console-link documentation style for operational triage.
- `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md` - Existing alert-policy source index and guidance that Terraform, not markdown, owns thresholds.

### Platform References
- `https://docs.cloud.google.com/logging/docs/export/pubsub` - Cloud Logging
  sink to Pub/Sub behavior and expected routing latency.
- `https://docs.cloud.google.com/pubsub/docs/push` - Pub/Sub push ACK/NACK
  semantics.
- `https://docs.cloud.google.com/pubsub/docs/dead-letter-topics` - Pub/Sub
  dead-letter topic behavior.
- `https://docs.cloud.google.com/pubsub/docs/monitoring` - Pub/Sub backlog and
  dead-letter monitoring metrics.
- `https://docs.cloud.google.com/run/docs/monitoring` - Cloud Run service
  health and performance monitoring.
- `https://docs.cloud.google.com/logging/docs/logs-based-metrics` - Log-based
  metric options for relay operational logs.

## Existing Code Insights

### Reusable Assets
- `feed_audit_notifications.emit_feed_audit_notification(...)` already emits
  the producer-side structured notification and isolates failures from callers.
- The relay already logs WD delivery success and failure with `event_id`,
  `feed_id`, `feed_revision`, status code, attempts, retryability, and response
  body without logging API keys or before/after payloads.
- The deployment route module already outputs the sink name, subscription name,
  push endpoint, and push invoker identity needed for verification and runbook
  steps.
- The deployment app module already contains Terraform patterns for
  `google_logging_metric`, `google_monitoring_alert_policy`, Pub/Sub backlog
  alerts, and dashboard JSON templates.
- Existing pipeline-triage docs provide a source-index/runbook style that keeps
  thresholds in Terraform and docs focused on navigation and diagnosis.

### Established Patterns
- Terraform is the source of truth for permanent alert policies, dashboard
  resources, IAM, Pub/Sub route resources, and environment-specific values.
- Public repo code owns reusable contracts and service behavior; deployment repo
  owns concrete Cloud Run services, Secret Manager wiring, route resources,
  environment names, and operator runbooks.
- Cloud Run/Pub/Sub paths use platform metrics for request counts, latencies,
  response codes, backlog, oldest unacked age, and dead-lettered messages.
- Existing operational docs avoid copying thresholds from markdown; they point
  operators to Terraform and live GCP for exact values.

### Integration Points
- Add relay operational log improvements in
  `backend/pipeline/feed_audit_webhook/main.py` or `wd_client.py` only if needed
  to satisfy `OPS-01`; do not duplicate payload logs.
- Add feed-audit-specific log metrics, alert policies, dashboard panels, or
  outputs in the deployment repo if existing generic monitoring does not prove
  the path clearly enough.
- Add rollout/runbook docs in the deployment repo because they depend on
  concrete sink/topic/subscription/service/secret/IAM names.
- If a reusable staging verification helper is useful without private
  environment assumptions, keep it in the public repo; otherwise put the
  environment-specific command/script in the deployment repo.

## Specific Ideas

- A good staging proof should create or trigger a real audited feed mutation,
  then verify the same `event_id` or `feed_id` through Logs Explorer, the
  Pub/Sub route/subscription, relay delivery logs, and the WD webhook response.
- DLQ proof should avoid production disruption. Prefer a staging-only controlled
  failure mode such as temporarily pointing the relay at a test endpoint or
  using a known invalid API key in a safe environment, then restoring config.
- Operational docs should include exact queries/commands for:
  Cloud Logging producer entries, sink route match, Pub/Sub subscription
  backlog, push delivery failures, relay Cloud Run logs, WD response logs, and
  DLQ message inspection.
- Alerting should stay minimal for v1: enough to catch route/relay failure and
  DLQ accumulation without creating noisy pages for every best-effort duplicate
  or transient retry.

## Deferred Ideas

- Replay selected `feed_audit_events` rows by event ID or time range remains
  v2 scope.
- Delivery attempt history outside Cloud Logging/Pub/Sub DLQ remains v2 scope.
- Admin UI/API delivery status for individual audit events remains v2 scope.
- Stronger outbound webhook authentication or key rotation without relay
  downtime remains v2 scope unless security requirements change.
- Multi-destination fanout remains v2 scope.

---

*Phase: 4-Operations and Rollout Proof*
*Context gathered: 2026-06-27*
