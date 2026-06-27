# Phase 04: Operations and Rollout Proof - Research

**Researched:** 2026-06-27
**Domain:** Google Cloud operations proof for Cloud Logging -> Pub/Sub -> Cloud Run relay -> Watch Duty webhook delivery
**Confidence:** HIGH for documented GCP behavior and repository state; MEDIUM for rollout sequencing because environment-specific staging/prod values are not stored in the public repo. [VERIFIED: .planning/config.json; VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]

<user_constraints>
## User Constraints (from CONTEXT.md)

The following locked decisions, discretion areas, and deferred ideas are copied from `.planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md`. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]

### Locked Decisions

#### Prior Decisions Carry Forward
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

#### Discussion Outcome
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

### Deferred Ideas (OUT OF SCOPE)

- Replay selected `feed_audit_events` rows by event ID or time range remains
  v2 scope.
- Delivery attempt history outside Cloud Logging/Pub/Sub DLQ remains v2 scope.
- Admin UI/API delivery status for individual audit events remains v2 scope.
- Stronger outbound webhook authentication or key rotation without relay
  downtime remains v2 scope unless security requirements change.
- Multi-destination fanout remains v2 scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OPS-01 | Producer, routing, relay success, relay retryable failure, relay permanent/config failure, and DLQ paths emit structured operational logs. | The producer helper already emits a structured log with the v1 payload; the relay already emits structured WD success/failure logs but malformed/config paths need structured fields; Pub/Sub DLQ forwarding is a managed platform event best observed through `subscription/dead_letter_message_count`, DLQ topic/subscription metrics, and DLQ message inspection rather than a custom DLQ consumer. [VERIFIED: backend/pipeline/storage/feed_audit_notifications.py; VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py; VERIFIED: backend/pipeline/feed_audit_webhook/main.py; CITED: https://docs.cloud.google.com/pubsub/docs/monitoring] |
| OPS-02 | Deployment configuration documents the required Cloud Logging sink, Pub/Sub topic, DLQ topic, push subscription, relay service, secret/env vars, and IAM bindings. | The deployment repo owns the sink, topics, DLQ subscription, push OIDC identity, Cloud Run service, Secret Manager key, workflow deployment, and outputs; Phase 4 should document these in the deployment repo and keep thresholds/config source-of-truth in Terraform. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md] |
| OPS-03 | Staging verification proves a real feed audit row can produce a Pub/Sub message and a WD webhook call without touching the feed write path. | Existing feed admin routes create/update/reset/deactivate/delete real audited feed rows through the BFF/feeds-service path; the proof should trigger one of these existing mutations and trace the resulting `event_id` through producer logs, routed LogEntry, Pub/Sub delivery, relay logs, and WD response logs. [VERIFIED: frontend/api/src/feeds/feedsController.ts; VERIFIED: backend/services/feeds/main.py; VERIFIED: backend/pipeline/storage/feed_audit_notifications.py; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub] |
| OPS-04 | Production rollout has a runbook for checking routed logs, Pub/Sub backlog, push failures, and DLQ messages. | Existing pipeline-triage docs define the runbook style: docs index live Terraform resources, use `gcloud`/Logs Explorer queries, include console deep links, and avoid copying mutable thresholds from markdown. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/SKILL.md; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/console-deep-links.md; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md] |
</phase_requirements>

## Summary

Phase 4 should be planned as an operations proof and rollout package, not as a delivery redesign. The delivery path is already constrained to producer structured logs, Cloud Logging sink routing, Pub/Sub push delivery, a stateless Cloud Run relay, and the WD webhook; the research found no need for DB polling, delivery tables, replay tooling, CDC, or write-path changes. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; VERIFIED: .planning/PROJECT.md; VERIFIED: .planning/REQUIREMENTS.md]

The most important planning finding is that the deployment app module still exposes `feed_audit_notification_route_enabled` with default `false`, and the dev/prod environment roots do not currently pass it. Phase 4 must either enable the route first in staging/dev before proof or make route enablement an explicit rollout step before the end-to-end verification. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/variables.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/environments/dev/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/environments/prod/main.tf]

**Primary recommendation:** Implement one deployment-repo operations slice: enable/verify the route in the staging environment, add minimal structured relay log hardening for invalid/config paths if required by OPS-01, add Terraform-owned metrics/alerts only for DLQ/relay failure signals that are not already clear, and write a production rollout runbook that traces a real `event_id` end to end. [VERIFIED: backend/pipeline/feed_audit_webhook/main.py; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/monitoring]

## Project Constraints (from AGENTS.md)

| Directive | Planning Impact |
|-----------|-----------------|
| Read `.agents/instructions.md` before code changes or reviews; style guides are required before code changes. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md] | Phase plans that edit Python/TypeScript must include the required style-guide reads; docs-only research does not need broad tests. [VERIFIED: .agents/instructions.md] |
| Do not run broad local tests by default; docs-only changes should use `git diff --check`; avoid local E2E/API/component/full integration tests unless explicitly approved. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md] | Plan verification should use focused unit/static checks and `safe-run`; staging proof should run against deployed GCP, not local Docker/testcontainers. [VERIFIED: AGENTS.md; VERIFIED: .agents/instructions.md] |
| Prefer `safe-run -- <command>` for tests/builds/installs/browser/e2e/benchmarks and other substantial workloads. [VERIFIED: user-provided AGENTS.md instructions; VERIFIED: command -v safe-run] | Any Terraform validation, pytest, or workflow-assisted proof commands should be wrapped with `safe-run` when run locally. [VERIFIED: .agents/instructions.md] |
| Prefer `mise` for standard formatting, linting, generation, and validation tasks. [VERIFIED: .agents/instructions.md] | Deployment repo checks should use existing `mise run check` or focused Terraform commands consistent with the repo. [VERIFIED: ../../feed-audit-notification-routing-deployment/README.md] |
| Do not bypass hooks with `--no-verify`; use semantic commit prefixes. [VERIFIED: .agents/instructions.md] | If Phase 4 commits code/docs, commit normally and avoid hook bypasses. [VERIFIED: .agents/instructions.md] |
| Current docs must be fetched with `ctx7` for libraries/frameworks/SDKs/APIs/CLIs/cloud services. [VERIFIED: user-provided AGENTS.md instructions; VERIFIED: ctx7 CLI output] | GCP operational behavior in this research was checked through Context7 and primary Google Cloud docs. [VERIFIED: ctx7 /websites/cloud_google_sdk; CITED: https://docs.cloud.google.com/pubsub/docs/push] |
| Notification logging/routing/webhook delivery must not add synchronous network calls, extra database reads, or failure coupling to ingestion and feed lifecycle writes. [VERIFIED: AGENTS.md project block; VERIFIED: .planning/PROJECT.md] | Staging proof must use an existing feed mutation externally and must not add write-path proof hooks. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md] |
| Pub/Sub push to the relay uses Cloud Run IAM/OIDC, and the relay authenticates to WD with the configured radio-transcription API key. [VERIFIED: AGENTS.md project block; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf] | Runbooks must document IAM/OIDC and Secret Manager wiring, but must not expose key values. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf] |

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Producer Feed Audit Notification emission | API / Backend storage layer | Cloud Logging | The producer helper emits structured logs after audited SQL returns a payload and swallows local emission failures. [VERIFIED: backend/pipeline/storage/feed_audit_notifications.py] |
| Log routing to delivery topic | Managed Cloud Logging | Pub/Sub topic | The route is a Log Router sink filtered on `jsonPayload.event_type` and `jsonPayload.schema_version`; Logging publishes matching entries to Pub/Sub. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub] |
| Push delivery and redelivery budget | Managed Pub/Sub | Cloud Run relay | Pub/Sub sends wrapped push requests, treats success status as ACK, treats non-success/timeout as retry, and owns retry/DLQ forwarding. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/push; CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics] |
| Webhook relay behavior | API / Backend Cloud Run service | Watch Duty backend | The relay validates the Pub/Sub LogEntry envelope, forwards the flat payload to WD, logs WD outcomes, and never reads/writes AlloyDB. [VERIFIED: backend/pipeline/feed_audit_webhook/main.py; VERIFIED: backend/pipeline/feed_audit_webhook/pubsub.py; VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py; VERIFIED: backend/pipeline/feed_audit_webhook/README.md] |
| Alerts, log-based metrics, dashboards | Operations / IaC | Cloud Monitoring | Permanent metrics, dashboards, alert policies, and thresholds are Terraform-owned in the deployment repo. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/monitoring.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md] |
| Staging proof and production runbook | Deployment repo docs/scripts | GCP Console / gcloud | Environment names, secrets, IAM bindings, console links, and operational commands are deployment-specific and should stay in the deployment repo. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; VERIFIED: ../../feed-audit-notification-routing-deployment/README.md] |

## Standard Stack

### Core

| Library / Service | Version | Purpose | Why Standard |
|-------------------|---------|---------|--------------|
| Cloud Logging Log Router sink | Managed service; docs last updated 2026-06-26 | Routes matching structured log entries to Pub/Sub. | Google documents Pub/Sub as a supported near-real-time sink destination and the existing module already implements a project sink. [CITED: https://docs.cloud.google.com/logging/docs/export/pubsub; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf] |
| Pub/Sub push subscription and DLQ | Managed service | Pushes routed LogEntry messages to Cloud Run, retries non-success responses, and forwards undeliverable messages to DLQ. | Pub/Sub push is the existing route shape, and DLQ behavior is configured on the subscription. [CITED: https://docs.cloud.google.com/pubsub/docs/push; CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf] |
| Cloud Run relay | Managed service | Hosts the stateless FastAPI relay and exposes platform metrics/logs. | The deployment module already defines `feed-audit-webhook-${environment}` with minimal runtime IAM and Secret Manager access. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf; CITED: https://docs.cloud.google.com/run/docs/monitoring] |
| Cloud Monitoring log-based metrics and alert policies | Managed service | Converts relay/platform logs into metrics and alert policies where platform metrics are insufficient. | Google supports log-derived counter/distribution metrics and the deployment repo already uses Terraform-managed alert patterns. [CITED: https://docs.cloud.google.com/logging/docs/logs-based-metrics; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/monitoring.tf] |
| Terraform + Google provider | Terraform required `>=1.3`; provider `hashicorp/google >=7.21.0`; local Terraform `1.15.0`; tool pin `1.14.5` | Owns permanent GCP resources, metrics, alert policies, dashboards, IAM, and environment wiring. | Existing deployment modules use Terraform as the source of truth and existing docs require Terraform PR/apply flow. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/versions.tf; VERIFIED: .tool-versions; VERIFIED: command terraform version] |
| gcloud CLI | Local `Google Cloud SDK 565.0.0` | Runs Logs Explorer equivalents, Pub/Sub pulls, Cloud Run log reads, and rollout verification commands. | Existing triage skill and README use `gcloud` for read-only operational checks. [VERIFIED: command gcloud --version; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/SKILL.md; VERIFIED: ../../feed-audit-notification-routing-deployment/README.md] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| FastAPI | `0.136.1` in `uv.lock` | Relay HTTP app runtime. | Only if Phase 4 adds relay log fields/tests; do not change app architecture. [VERIFIED: uv.lock; VERIFIED: backend/pipeline/feed_audit_webhook/pyproject.toml] |
| Uvicorn | `0.46.0` in `uv.lock` | ASGI server for relay container. | Existing relay runtime; no Phase 4 change expected. [VERIFIED: uv.lock; VERIFIED: backend/pipeline/feed_audit_webhook/Dockerfile] |
| urllib3 | `2.7.0` in `uv.lock` | WD webhook HTTP client. | Existing WD client uses it; do not introduce another HTTP dependency for operations-only changes. [VERIFIED: uv.lock; VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py] |
| google-cloud-logging | `3.15.0` in `uv.lock` | Existing Python Cloud Logging integration through `setup_logging()`. | Keep using existing logging helper; do not hand-create Cloud Logging clients in producer/relay operations code. [VERIFIED: uv.lock; VERIFIED: backend/pipeline/common/log_helper.py] |
| GitHub CLI | Local `2.45.0` | Inspect and trigger deployment workflows when needed. | Use for workflow status/run inspection if rollout proof uses GitHub Actions. [VERIFIED: command gh --version; VERIFIED: ../../feed-audit-notification-routing-deployment/.github/workflows/app_deploy.yml] |
| jq | Local `1.7` | Decode/format Pub/Sub/LogEntry JSON during runbook checks. | Use in runbook snippets for base64-decoded `message.data` and Terraform/GCP command output. [VERIFIED: command jq --version] |
| safe-run | Present at `/home/shuojing/.local/bin/safe-run` | Host-stability wrapper for heavier local commands. | Wrap local Terraform/test/build checks per AGENTS instructions. [VERIFIED: command -v safe-run; VERIFIED: user-provided AGENTS.md instructions] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Managed Pub/Sub DLQ inspection | Custom DLQ consumer that logs every forwarded message | A custom consumer would create new runtime behavior and a new delivery surface; use Pub/Sub metrics plus pull-based inspection for v1. [CITED: https://docs.cloud.google.com/pubsub/docs/monitoring; VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md] |
| Terraform-owned alerts | Console-created alert policies | Console-created alerts drift from source; the deployment repo explicitly says Terraform/live GCP are source of truth and docs should not copy thresholds. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md] |
| Existing Cloud Run/Pub/Sub platform metrics | Custom in-process counters for request count/backlog/DLQ | Cloud Run and Pub/Sub already expose request, latency, backlog, and DLQ metrics; custom metrics add code paths without improving v1 proof. [CITED: https://docs.cloud.google.com/run/docs/monitoring; CITED: https://docs.cloud.google.com/pubsub/docs/monitoring] |
| Existing feed admin mutation for proof | Special audit-writing proof endpoint | A special endpoint would touch the write path and contradict the locked verification boundary. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; VERIFIED: backend/services/feeds/main.py] |

**Installation:** No new package installation is recommended for Phase 4. Use the existing Python lockfile, deployment Terraform provider constraints, and installed `gcloud`/Terraform/`gh`/`jq` tools. [VERIFIED: uv.lock; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/versions.tf; VERIFIED: local environment audit]

**Version verification:** Python package versions were verified from `uv.lock`, Terraform/provider constraints from deployment `versions.tf`, and CLI versions from local commands; npm registry checks are not applicable because Phase 4 is GCP/Python/Terraform operations work, not a new npm package decision. [VERIFIED: uv.lock; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/versions.tf; VERIFIED: command gcloud --version; VERIFIED: command terraform version; VERIFIED: command gh --version]

## Architecture Patterns

### System Architecture Diagram

```text
Real feed admin mutation or ingestion lifecycle event
        |
        v
feed_audit_events row inserted in existing storage transaction
        |
        v
Producer structured log:
  jsonPayload.event_type="radio_transcription.feed_audit_notification"
  jsonPayload.schema_version=1
        |
        v
Cloud Logging Log Router sink
  Decision: does jsonPayload match event_type + schema_version?
        | yes
        v
Dedicated Pub/Sub notification topic
        |
        v
Authenticated Pub/Sub push subscription
  Decision: relay HTTP status is success before ack deadline?
        | 2xx / 204                     | non-2xx or timeout
        v                               v
Cloud Run relay forwards to WD      Pub/Sub retry policy
        |                               |
        v                               v
WD webhook response logs          max delivery attempts reached?
        |                               | yes
        v                               v
Operator proof by event_id        DLQ topic + DLQ subscription
        |                               |
        v                               v
Rollout/runbook records          DLQ inspection and alert path
```

This diagram follows the locked delivery path and excludes outbox, DB polling, direct WD calls from writes, replay tooling, and delivery-state tables. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; VERIFIED: backend/pipeline/feed_audit_webhook/main.py]

### Recommended Project Structure

```text
radio-transcription/
  backend/pipeline/feed_audit_webhook/          # Only minimal relay log/test hardening if OPS-01 needs it
  .planning/phases/04-operations-and-rollout-proof/04-RESEARCH.md

feed-audit-notification-routing-deployment/
  docs/
    feed-audit-notification-rollout.md         # Deployment config, staging proof, prod rollout checklist
  .claude/skills/pipeline-triage/
    console-deep-links.md                      # Add feed audit relay/topic/subscription/DLQ links
    alert-policies.md                          # Add source-index row for feed audit notification alerts
    triage-flows/feed-audit-notification.md    # Operator diagnosis flow for route, relay, WD, DLQ
  terraform/modules/app/
    main.tf                                    # Route enablement, alert policies, metric wiring if added
    monitoring.tf                              # Existing log-based metric/alert pattern if used
    dashboards/system_health_overview.json.tftpl # Optional dashboard panel update if v1 needs it
```

This split keeps reusable application contracts in the public repo and concrete environment/IaC/runbook content in the deployment repo. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/SKILL.md]

### Pattern 1: Structured Operational Log Taxonomy

**What:** Use stable low-cardinality `relay_event` values and event identifiers in relay logs; do not log secrets or full before/after payloads in delivery outcome logs. [VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py; VERIFIED: backend/pipeline/feed_audit_webhook/tests/test_wd_client.py]

**When to use:** Use this when Phase 4 adds missing structured fields for invalid Pub/Sub messages, uninitialized WD client/config failure, or permanent relay failures. [VERIFIED: backend/pipeline/feed_audit_webhook/main.py]

**Recommended fields:** `relay_event`, `event_id`, `feed_id`, `feed_revision`, `wd_status_code`, `attempts`, `retryable`, `failure_class`, and sanitized `wd_response_body` when WD responds with an error. [VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py; VERIFIED: backend/pipeline/feed_audit_webhook/tests/test_wd_client.py]

**Example:**

```python
# Source: backend/pipeline/feed_audit_webhook/wd_client.py
logger.info(
    "Feed Audit Notification delivered to Watch Duty",
    extra={
        "json_fields": {
            "relay_event": "feed_audit_webhook_delivery",
            "event_id": payload.get("event_id"),
            "feed_id": payload.get("feed_id"),
            "feed_revision": payload.get("feed_revision"),
            "wd_status_code": status_code,
            "attempts": attempt,
        }
    },
)
```

The exact source uses `_log_fields(...)`; this example shows the same established field shape for planner readability. [VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py]

### Pattern 2: Terraform-Owned Alerting

**What:** Add alert policies and log-based metrics in Terraform, and add only source-index/runbook references in markdown. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/monitoring.tf]

**When to use:** Use this for relay failure count, invalid message count, DLQ forwarded message count, or push subscription backlog if existing generic monitoring does not make the feed audit path visible enough. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/monitoring]

**Example:**

```hcl
# Source pattern: terraform/modules/app/monitoring.tf and terraform/modules/app/main.tf
resource "google_logging_metric" "feed_audit_webhook_failures" {
  project     = var.project_id
  name        = "feed_audit_webhook_failures"
  description = "Feed Audit Notification relay failures."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND resource.labels.service_name="feed-audit-webhook-${var.environment}"
    AND jsonPayload.relay_event="feed_audit_webhook_delivery"
    AND jsonPayload.retryable=FALSE
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}
```

Add `time_sleep` between new log-based metrics and alert policies if following existing propagation-delay pattern. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/monitoring.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf]

### Pattern 3: Staging Proof By Event ID

**What:** Trigger a normal feed admin mutation, capture the resulting `event_id`, and follow it through producer logs, sink-routed Pub/Sub LogEntry, relay delivery logs, and WD response. [VERIFIED: backend/services/feeds/main.py; VERIFIED: frontend/api/src/feeds/feedsController.ts; VERIFIED: backend/pipeline/storage/feed_audit_notifications.py]

**When to use:** Use this as the Phase 4 end-to-end proof; do not add a fake emitter, special endpoint, delivery table, or replay function. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]

**Example log query:**

```text
jsonPayload.event_type="radio_transcription.feed_audit_notification"
jsonPayload.schema_version=1
jsonPayload.feed_id="<feed_id>"
timestamp >= "<proof_start_time>"
```

This query is for Logs Explorer or `gcloud logging read`; it follows the same structured fields the sink uses for routing. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]

### Anti-Patterns to Avoid

- **Adding delivery state to `feed_audit_events` or a new table:** This violates locked scope and creates a second delivery ledger. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]
- **Creating a DLQ drain/replay service in v1:** Replay and delivery attempt history are deferred v2 ideas. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]
- **Using exact Pub/Sub DLQ attempt count as a hard assertion:** Pub/Sub documents dead-letter forwarding as best-effort around the configured max attempts. [CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics]
- **Copying alert thresholds into markdown:** The pipeline-triage skill explicitly says Terraform/live GCP are the source of truth. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md]
- **Adding high-cardinality log metric labels from response bodies or before/after snapshots:** Existing tests prevent payload snapshots in delivery failure logs, and log-based metric labels create separate time series per label combination. [VERIFIED: backend/pipeline/feed_audit_webhook/tests/test_wd_client.py; CITED: https://docs.cloud.google.com/logging/docs/logs-based-metrics]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Delivery durability | Outbox table, cursor table, CDC, DB polling, trigger, `LISTEN/NOTIFY` | Existing `feed_audit_events` ledger plus Cloud Logging/Pub/Sub route | Durable audit history is already in the ledger; v1 notifications are best-effort operational signals. [VERIFIED: .planning/PROJECT.md; VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md] |
| Push authentication | Custom shared secret on Pub/Sub push | Cloud Run IAM/OIDC push invoker service account | Existing route uses Pub/Sub OIDC and Cloud Run `roles/run.invoker`; Google documents authenticated push JWT behavior. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions] |
| DLQ monitoring | Custom DLQ consumer for v1 | Pub/Sub `subscription/dead_letter_message_count`, DLQ topic/subscription metrics, and pull-based inspection | Google exposes forwarded-message and DLQ backlog metrics, and the repo already creates a DLQ subscription for inspection. [CITED: https://docs.cloud.google.com/pubsub/docs/monitoring; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf] |
| Cloud Run health metrics | In-process request counters | Cloud Run built-in request count, latency, CPU, memory, instance count | Cloud Run exposes built-in metrics automatically. [CITED: https://docs.cloud.google.com/run/docs/monitoring] |
| Permanent alert policy state | Console-created alerts | Terraform resources in deployment repo | Existing operational guidance says Terraform/live GCP are source of truth. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md] |
| Staging proof | Fake payload injector or special write-path hook | Existing feed API mutation that writes a real audit row | Phase 4 must observe the route externally without touching feed write path delivery behavior. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; VERIFIED: backend/services/feeds/main.py] |

**Key insight:** The hard parts in this domain are managed-platform semantics, IAM boundaries, and operator diagnosis, not message transformation; custom delivery infrastructure would add more failure modes than it removes for v1. [VERIFIED: .planning/PROJECT.md; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics]

## Common Pitfalls

### Pitfall 1: Proving A Disabled Route

**What goes wrong:** The team triggers a real audit row but no Pub/Sub push happens because `feed_audit_notification_route_enabled` remains false in the environment root. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/variables.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/environments/dev/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/environments/prod/main.tf]

**Why it happens:** Phase 2 intentionally made the route disabled by default until a real relay existed, and Phase 3 wired the relay service but did not flip the environment roots. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-03-SUMMARY.md; VERIFIED: .planning/phases/03-webhook-relay-delivery/03-04-SUMMARY.md]

**How to avoid:** Make route enablement or live-route verification Wave 0 of Phase 4 before staging proof. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf]

**Warning signs:** App outputs for sink/subscription are null, no `feed-audit-notification-route-${environment}` sink exists, or Pub/Sub topic send metrics stay flat after producer logs appear. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/outputs.tf; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]

### Pitfall 2: Treating DLQ Attempt Count As Exact

**What goes wrong:** A test or runbook expects exactly ten failed pushes before the message appears in DLQ. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf]

**Why it happens:** The Terraform config sets `max_delivery_attempts = 10`, but Pub/Sub documents the forwarding count as approximate and dependent on correct DLQ IAM. [CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics]

**How to avoid:** Verify that a controlled poison message eventually appears in the DLQ and that monitoring detects forwarded messages, not that the attempt counter equals ten. [CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics; CITED: https://docs.cloud.google.com/pubsub/docs/monitoring]

**Warning signs:** Tests assert exact attempt numbers or fail because forwarding occurs slightly before/after the configured limit. [CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics]

### Pitfall 3: Missing Structured Fields On Invalid Relay Paths

**What goes wrong:** WD success/failure logs are queryable, but malformed Pub/Sub messages or config/client initialization issues only emit plain warning text. [VERIFIED: backend/pipeline/feed_audit_webhook/main.py; VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py]

**Why it happens:** Phase 3 focused on relay delivery semantics; `wd_client.py` has structured fields, while `main.py` invalid-message warnings are not yet structured. [VERIFIED: backend/pipeline/feed_audit_webhook/main.py; VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py]

**How to avoid:** Add `json_fields` to invalid-message and client-not-initialized logs without logging full payloads or API keys. [VERIFIED: backend/pipeline/feed_audit_webhook/tests/test_wd_client.py]

**Warning signs:** Logs Explorer queries by `jsonPayload.relay_event` show delivery failures but cannot distinguish parser/config failures. [VERIFIED: backend/pipeline/feed_audit_webhook/main.py]

### Pitfall 4: High-Cardinality Metrics

**What goes wrong:** Log-based metrics label on `event_id`, `feed_id`, response body, or arbitrary error text and create excessive time series. [ASSUMED]

**Why it happens:** Log-based metric labels create separate time series for each label combination. [CITED: https://docs.cloud.google.com/logging/docs/logs-based-metrics]

**How to avoid:** Use low-cardinality labels such as `failure_class` and `retryable`; keep `event_id` in logs for diagnosis, not alert metric labels. [VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py; CITED: https://docs.cloud.google.com/logging/docs/logs-based-metrics]

**Warning signs:** Terraform metric label extractors include UUIDs, full status text, response bodies, or dynamic endpoint values. [CITED: https://docs.cloud.google.com/logging/docs/logs-based-metrics]

### Pitfall 5: Alerting From Docs Instead Of Terraform

**What goes wrong:** Runbook thresholds drift from Terraform, or an operator trusts markdown after policy changes. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md]

**Why it happens:** Markdown is easier to edit than Terraform but is not the source of truth for live alert filters and thresholds. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md]

**How to avoid:** Put exact filters/thresholds in Terraform and reference source files or live GCP policy IDs from docs. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md]

**Warning signs:** A runbook hardcodes alert threshold values instead of pointing to Terraform or `gcloud alpha monitoring policies describe`. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md]

## Code Examples

Verified patterns from source and official docs:

### Producer Log Query

```text
jsonPayload.event_type="radio_transcription.feed_audit_notification"
jsonPayload.schema_version=1
jsonPayload.feed_id="<feed_id>"
timestamp >= "<proof_start_time>"
```

The sink uses the same `jsonPayload.event_type` and `jsonPayload.schema_version` fields, so this query proves the producer side and identifies candidate `event_id` values. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; VERIFIED: backend/pipeline/storage/feed_audit_notifications.py]

### Relay Delivery Query

```text
resource.type="cloud_run_revision"
resource.labels.service_name="feed-audit-webhook-<env>"
jsonPayload.relay_event="feed_audit_webhook_delivery"
jsonPayload.event_id="<event_id>"
```

This query uses the relay's existing structured delivery log fields. [VERIFIED: backend/pipeline/feed_audit_webhook/wd_client.py]

### Pub/Sub DLQ Inspection

```bash
PROJECT="$(gcloud config get-value project)"
gcloud pubsub subscriptions pull "feed-audit-notification-dlq-subscription-<env>" \
  --auto-ack \
  --limit=10 \
  --project="$PROJECT"
```

The deployment repo creates the feed audit DLQ subscription with seven-day message retention, and existing triage docs use pull-based DLQ sampling. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/triage-flows/perf-regression.md]

### Decode Routed LogEntry From Pub/Sub

```bash
gcloud pubsub subscriptions pull "<temporary-or-debug-subscription>" \
  --limit=1 \
  --format='value(message.data)' |
base64 --decode |
jq '.jsonPayload'
```

Cloud Logging routes LogEntry objects through Pub/Sub with `message.data` base64-encoded; decode before inspecting `jsonPayload`. [CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]

### Terraform Route Enablement Check

```bash
rg -n 'feed_audit_notification_route_enabled\\s*=\\s*true' terraform/environments terraform/modules
terraform -chdir=terraform/environments/dev output feed_audit_notification_subscription_name
terraform -chdir=terraform/environments/dev output feed_audit_webhook_service_url
```

The first command checks code intent; the outputs confirm whether the app module exposes real route and relay values after apply. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/outputs.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/environments/dev/main.tf]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Application code publishes notifications directly from feed writes | Structured logs routed by Cloud Logging to Pub/Sub | Locked in this milestone Phase 1/2 | Keeps feed writes isolated from WD/network failures. [VERIFIED: .planning/phases/01-audit-contract-and-emission/01-CONTEXT.md; VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md] |
| Custom retry/delivery ledger | Pub/Sub retry policy plus DLQ | Locked in Phase 2 | Bounds poison/config retries without adding a DB delivery table. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf] |
| Hand-maintained dashboard JSON as primary truth | Terraform-managed dashboard template plus GCP auto-dashboards/deep links | Existing deployment repo pattern | Operators use auto-rendered resource dashboards and source-controlled templates while thresholds stay in Terraform. [VERIFIED: ../../feed-audit-notification-routing-deployment/README.md; VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/console-deep-links.md] |
| Console-created alert policies | Terraform alert policies with markdown source indexes | Existing pipeline-triage policy | Prevents drift and makes PR review the change path for permanent alerts. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md] |

**Deprecated/outdated:**
- Direct WD calls from ingestion/feed writes are out of scope because they couple critical writes to downstream availability. [VERIFIED: .planning/PROJECT.md; VERIFIED: .planning/REQUIREMENTS.md]
- DB polling, CDC, triggers, `LISTEN/NOTIFY`, outbox payload tables, and delivery status UI/API are out of scope for v1. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]
- Exact DLQ delivery-attempt assertions are unsafe because Pub/Sub forwards dead-letter messages on a best-effort basis around the configured maximum. [CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | A staging/dev environment is available and acceptable for the end-to-end proof before production rollout. [ASSUMED] | Summary, Environment Availability, Open Questions | If no staging/dev environment is available, OPS-03 needs a different safe proof target or user approval to prove in production. |
| A2 | High-cardinality log metric labels such as event IDs and response bodies should be avoided. [ASSUMED] | Common Pitfalls | If the project's Monitoring quota/cost posture allows it, the plan might be too conservative, but the safer default is low-cardinality labels. |

## Open Questions (RESOLVED)

1. **Should Phase 4 flip `feed_audit_notification_route_enabled` in dev/staging and prod, or only document the enablement step?**
   - What we know: The app module flag defaults to false and environment roots currently do not pass it. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/variables.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/environments/dev/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/environments/prod/main.tf]
   - Resolution: Phase 4 should plan concrete route enablement/config where appropriate. Dev/staging should be enabled and verified before the live proof, while production rollout remains gated by the rollout docs/runbook when environment-specific values or operator action are required. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]
   - Planning impact: Plan environment-level route posture explicitly: enable dev/staging for OPS-03 proof, keep production activation tied to the production checklist, Terraform plan review, and post-deploy operator verification. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-02-PLAN.md]

2. **What exact WD staging endpoint and API key should receive the proof event?**
   - What we know: The concrete staging WD backend origin/secret are environment-specific and must not be stored in public docs. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]
   - Resolution: Phase 4 should document the required deployment variable/secret contract and require staging proof to use environment-provided `WD_BACKEND_ENDPOINT` / `WD_BACKEND_ENDPOINT_API_KEY` or the configured feed-audit webhook Secret Manager secret. Secret values must remain in the deployment environment/secret store, not markdown. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf]
   - Planning impact: Rollout docs should name the variable/secret contract, show redacted placeholders only, and instruct operators to verify the configured endpoint/key source without printing or copying secret values into logs or docs. [VERIFIED: ../../feed-audit-notification-routing-deployment/README.md]

3. **Does OPS-01 require a custom log entry when Pub/Sub forwards to DLQ?**
   - What we know: Pub/Sub exposes a dead-letter forwarded-message metric and the DLQ topic/subscription can be inspected. [CITED: https://docs.cloud.google.com/pubsub/docs/monitoring; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf]
   - Resolution: Pub/Sub DLQ platform metrics plus DLQ subscription/message inspection are acceptable for OPS-01. No custom DLQ log producer, custom DLQ consumer, replay service, or delivery table is required. [CITED: https://docs.cloud.google.com/pubsub/docs/monitoring; VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md]
   - Planning impact: Plans should use `pubsub.googleapis.com/subscription/dead_letter_message_count`, DLQ backlog metrics, and pull-based inspection from the existing DLQ subscription as the operational proof for DLQ behavior. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| gcloud CLI | Runbook and staging/prod verification | Yes | Google Cloud SDK 565.0.0 | Use GCP Console for manual inspection if CLI auth is missing. [VERIFIED: command gcloud --version] |
| Terraform | Deployment repo route/alert/dashboard changes | Yes | Local 1.15.0; project pin 1.14.5 | Use `mise` to install pinned version if exact local parity is required; use GitHub Actions plan for authoritative validation. [VERIFIED: command terraform version; VERIFIED: .tool-versions; VERIFIED: ../../feed-audit-notification-routing-deployment/README.md] |
| GitHub CLI | Workflow inspection/triggering if rollout proof uses Actions | Yes | 2.45.0 | Use GitHub UI Actions tab. [VERIFIED: command gh --version; VERIFIED: ../../feed-audit-notification-routing-deployment/README.md] |
| jq | Decode/filter Pub/Sub and gcloud JSON output | Yes | 1.7 | Use Python `json.tool` for local formatting if needed. [VERIFIED: command jq --version] |
| mise | Standard repo validation tasks | Yes | 2026.3.18 linux-x64 | Run focused tool commands directly if mise task is not applicable. [VERIFIED: command mise --version; VERIFIED: .agents/instructions.md] |
| safe-run | Host-stable local validation wrapper | Yes | Present, no version output | Run low-resource commands directly only when safe-run is unavailable. [VERIFIED: command -v safe-run; VERIFIED: user-provided AGENTS.md instructions] |
| GCP credentials/project selection | Live route proof, Pub/Sub pulls, log reads | Unknown | Not checked to avoid mutating or depending on local auth state | Run proof in CI/GitHub Actions or have operator authenticate locally. [ASSUMED] |

**Missing dependencies with no fallback:**
- None found for planning; live proof still requires environment-specific GCP access and WD credentials. [VERIFIED: local environment audit; ASSUMED]

**Missing dependencies with fallback:**
- Exact Terraform pin mismatch: local Terraform is newer than `.tool-versions`; use `mise` or CI for pin-accurate validation. [VERIFIED: command terraform version; VERIFIED: .tool-versions]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | yes | Pub/Sub push uses OIDC with a dedicated push invoker service account; Cloud Run service invocation is gated by `roles/run.invoker`; relay authenticates to WD with `X-Api-Key` from Secret Manager. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions] |
| V3 Session Management | no | Phase 4 does not add browser sessions or user-session state. [VERIFIED: .planning/REQUIREMENTS.md] |
| V4 Access Control | yes | Keep IAM least privilege: sink writer topic-scoped publisher, Pub/Sub service agent token/DLQ roles, relay service account secret/log/monitor/trace roles only. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf] |
| V5 Input Validation | yes | Relay validates Pub/Sub envelope, base64 JSON LogEntry, `jsonPayload`, event type, schema version, and required v1 fields before forwarding. [VERIFIED: backend/pipeline/feed_audit_webhook/pubsub.py; VERIFIED: backend/pipeline/feed_audit_webhook/tests/test_pubsub.py] |
| V6 Cryptography | yes | Use managed HTTPS/OIDC/Secret Manager; do not hand-roll JWT signing or custom push auth. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service] |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Spoofed Pub/Sub push to relay | Spoofing | Keep Cloud Run private behind IAM and use Pub/Sub OIDC push identity with `roles/run.invoker`. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions] |
| Overbroad log sink publisher permissions | Elevation of Privilege | Grant sink writer `roles/pubsub.publisher` only on the notification topic. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub] |
| WD API key leakage in logs | Information Disclosure | Keep key in Secret Manager and tests ensure API key is not logged on WD failure. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf; VERIFIED: backend/pipeline/feed_audit_webhook/tests/test_wd_client.py] |
| Poison messages repeatedly failing relay validation | Denial of Service | Return non-2xx so Pub/Sub retry/DLQ policy bounds repeated delivery; inspect DLQ messages. [VERIFIED: backend/pipeline/feed_audit_webhook/main.py; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics] |
| Sensitive audit snapshots exposed through operational logs | Information Disclosure | Delivery logs should include event/feed identifiers and WD response body but not `before_values` or `after_values`; existing tests cover this for WD failures. [VERIFIED: backend/pipeline/feed_audit_webhook/tests/test_wd_client.py] |
| Alert drift or disabled notification channel | Repudiation / Detection Gap | Terraform owns alert policies/channels; runbooks should include drift/audit commands and not hardcode mutable thresholds. [VERIFIED: ../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md] |

## Sources

### Primary (HIGH confidence)

- `.planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md` - locked decisions, scope boundaries, canonical references. [VERIFIED]
- `.planning/REQUIREMENTS.md` - OPS-01 through OPS-04 and out-of-scope v2 items. [VERIFIED]
- `.planning/ROADMAP.md` - Phase 4 goal and success criteria. [VERIFIED]
- `.planning/STATE.md` - accumulated Phase 1-3 decisions. [VERIFIED]
- `backend/pipeline/storage/feed_audit_notifications.py` - producer structured log helper. [VERIFIED]
- `backend/pipeline/common/feed_audit_notification_contract.py` - event type, schema version, required fields. [VERIFIED]
- `backend/pipeline/feed_audit_webhook/main.py`, `pubsub.py`, `wd_client.py`, `README.md`, and tests - relay ACK/NACK, validation, logging, and WD behavior. [VERIFIED]
- `../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf` - sink, push, OIDC, retry, DLQ, IAM. [VERIFIED]
- `../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf` - feed audit notification topic, DLQ topic, DLQ subscription. [VERIFIED]
- `../../feed-audit-notification-routing-deployment/terraform/modules/services/feed_audit_webhook/main.tf` - Cloud Run relay service, Secret Manager key, runtime IAM. [VERIFIED]
- `../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf`, `variables.tf`, `outputs.tf`, `monitoring.tf` - app module composition, disabled route flag, outputs, monitoring patterns. [VERIFIED]
- `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/*` - runbook/triage/alert style. [VERIFIED]
- Context7 `/websites/cloud_google_sdk` - `gcloud logging` command group and Cloud Run log command availability. [VERIFIED: ctx7 CLI]
- Google Cloud docs: Logging to Pub/Sub, Pub/Sub push, Pub/Sub DLQ, Pub/Sub monitoring, Cloud Run monitoring, log-based metrics, push auth, Cloud Run service-to-service auth. [CITED: https://docs.cloud.google.com/logging/docs/export/pubsub; CITED: https://docs.cloud.google.com/pubsub/docs/push; CITED: https://docs.cloud.google.com/pubsub/docs/dead-letter-topics; CITED: https://docs.cloud.google.com/pubsub/docs/monitoring; CITED: https://docs.cloud.google.com/run/docs/monitoring; CITED: https://docs.cloud.google.com/logging/docs/logs-based-metrics; CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service]

### Secondary (MEDIUM confidence)

- Local CLI availability audit for `gcloud`, Terraform, `gh`, `jq`, `mise`, and `safe-run`. [VERIFIED: local commands]
- Prior Phase 1/2/3 context, research, and summaries for decision history. [VERIFIED: .planning/phases/01-audit-contract-and-emission; VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing; VERIFIED: .planning/phases/03-webhook-relay-delivery]

### Tertiary (LOW confidence)

- Environment-specific availability of authenticated GCP access, WD staging endpoint, and WD staging API key was not verified in this session. [ASSUMED]

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - repository files, lockfiles, Terraform, CLI versions, Context7, and primary Google Cloud docs were checked. [VERIFIED: uv.lock; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/versions.tf; VERIFIED: ctx7 CLI; CITED: https://docs.cloud.google.com/pubsub/docs/push]
- Architecture: HIGH - delivery path is locked by CONTEXT.md and implemented across public/deployment repos. [VERIFIED: .planning/phases/04-operations-and-rollout-proof/04-CONTEXT.md; VERIFIED: backend/pipeline/feed_audit_webhook/main.py; VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf]
- Pitfalls: MEDIUM - route enablement and current log-field gaps were verified; high-cardinality metric caution is an assumed best practice reinforced by log-based metric label behavior. [VERIFIED: ../../feed-audit-notification-routing-deployment/terraform/modules/app/variables.tf; VERIFIED: backend/pipeline/feed_audit_webhook/main.py; CITED: https://docs.cloud.google.com/logging/docs/logs-based-metrics; ASSUMED]

**Research date:** 2026-06-27
**Valid until:** 2026-07-27 for repository structure and current Google Cloud docs; re-check before rollout if GCP Pub/Sub/Cloud Run/Logging behavior or deployment repo modules change. [ASSUMED]
