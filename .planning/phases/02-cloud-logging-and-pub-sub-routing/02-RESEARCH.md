# Phase 2: Cloud Logging and Pub/Sub Routing - Research

**Researched:** 2026-06-26 [VERIFIED: environment current date]
**Domain:** Google Cloud Logging Log Router to Pub/Sub, Terraform Google provider, authenticated Pub/Sub push to Cloud Run [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions]
**Confidence:** HIGH for Terraform/GCP resource shape and deployment repo placement; MEDIUM for final app-module instantiation timing because Phase 3 relay resources do not exist yet [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4; VERIFIED: .planning/ROADMAP.md]

<user_constraints>
## User Constraints (from CONTEXT.md)

Source: [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

### Locked Decisions

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
  agent IAM for token creation and dead-letter behavior, retry backoff, and
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

### Deferred Ideas (OUT OF SCOPE)

- Implementing the Cloud Run relay application belongs to Phase 3.
- Parsing Pub/Sub `LogEntry` envelopes, extracting `jsonPayload`, and calling
  the Watch Duty webhook belongs to Phase 3.
- Operational dashboards, route proof, DLQ runbooks, and staging/prod rollout
  verification belong to Phase 4.
- Any future replay API, delivery-status UI, or multi-destination fanout remains
  v2 scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

Source: [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/ROADMAP.md]

| ID | Description | Research Support |
|----|-------------|------------------|
| ROUTE-01 | Cloud Logging routes Feed Audit Notification logs to a dedicated Pub/Sub topic with a filter on `jsonPayload.event_type` and `jsonPayload.schema_version`. | Use `google_logging_project_sink` with `destination = "pubsub.googleapis.com/projects/${project}/topics/${topic}"` and the exact event-contract filter. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown; CITED: https://docs.cloud.google.com/logging/docs/export/configure_export_v2; VERIFIED: backend/pipeline/storage/feed_audit_notifications.py] |
| ROUTE-02 | The Log Router sink writer has the minimal Pub/Sub publisher IAM needed for the notification topic. | Use the sink `writer_identity` as the member in a non-authoritative `google_pubsub_topic_iam_member` with `roles/pubsub.publisher` on only the notification topic. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_topic_iam.html.markdown] |
| ROUTE-03 | The Pub/Sub push subscription invokes the relay through authenticated Cloud Run IAM/OIDC. | Use `google_pubsub_subscription.push_config.oidc_token`, a dedicated push invoker service account, `roles/run.invoker` on the relay Cloud Run service, and Pub/Sub service-agent `roles/iam.serviceAccountTokenCreator`. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf] |
| ROUTE-04 | The Pub/Sub subscription uses retry backoff with 10 second minimum, 60 second maximum, and a dead-letter policy with 10 delivery attempts. | Set `retry_policy.minimum_backoff = "10s"`, `retry_policy.maximum_backoff = "60s"`, and `dead_letter_policy.max_delivery_attempts = 10`; the REST API allows max delivery attempts from 5 to 100 and retry backoff values up to 600 seconds. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown] |
</phase_requirements>

## Summary

Phase 2 is a deployment-infrastructure phase, not a producer or relay-runtime phase. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md] Phase 1 already emits structured logs with `event_type="radio_transcription.feed_audit_notification"` and numeric `schema_version=1`; Phase 2 should route exactly that contract through Cloud Logging and Pub/Sub without adding application delivery clients. [VERIFIED: backend/pipeline/storage/feed_audit_notifications.py; VERIFIED: backend/pipeline/storage/tests/test_feed_audit_notifications.py]

The route should be implemented in `radio-transcription-deployment`, not as a public Terraform module, because it depends on deployment-only concepts: environment composition, existing `message_queues`, Cloud Run relay service name/URL, project IAM, and CI deployment variables. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/main.tf] The public repo should remain the event-contract owner. [VERIFIED: backend/pipeline/storage/feed_audit_notifications.py; VERIFIED: .planning/REQUIREMENTS.md]

**Primary recommendation:** Add feed-audit notification topic/DLQ resources to deployment `terraform/modules/message_queues`, add a small deployment-owned `terraform/modules/feed_audit_notification_route` module for the sink/IAM/subscription, and call it from `terraform/modules/app` once Phase 3 relay outputs are available or as a required relay-input contract. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf; VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|--------------|----------------|-----------|
| Feed Audit Notification emission | API / Backend | Cloud Logging | Phase 1 emits structured logs through Python stdlib logging and must stay failure-isolated from feed writes. [VERIFIED: backend/pipeline/storage/feed_audit_notifications.py; VERIFIED: .planning/REQUIREMENTS.md] |
| Log filtering and fan-in | Cloud Logging / Log Router | Pub/Sub | A project-level Log Router sink owns matching `jsonPayload` fields and routes matching `LogEntry` records to Pub/Sub. [CITED: https://docs.cloud.google.com/logging/docs/export/configure_export_v2; VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md] |
| Notification topic and DLQ storage | Pub/Sub | Terraform deployment module | Existing deployment topology centralizes pipeline topics and DLQ retention subscriptions in `message_queues`. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf] |
| Sink writer publisher IAM | Pub/Sub IAM | Cloud Logging sink | The Logging sink exposes `writer_identity`; destination write access must be granted on the Pub/Sub topic. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown; CITED: https://docs.cloud.google.com/logging/docs/export/configure_export_v2] |
| Authenticated push to relay | Pub/Sub subscription | Cloud Run IAM | Pub/Sub signs an OIDC JWT as the configured push auth service account, and Cloud Run authorizes that service account through `roles/run.invoker`. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service] |
| Retry and DLQ behavior | Pub/Sub subscription | Pub/Sub service agent IAM | Subscription `retry_policy` and `dead_letter_policy` own redelivery bounds; Pub/Sub service agent needs publisher on the DLQ topic and subscriber/ack permissions on the source subscription. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf] |
| Relay application behavior | Phase 3 Cloud Run relay | Watch Duty webhook | Parsing Pub/Sub `LogEntry`, validating `jsonPayload`, and calling WD are explicitly deferred to Phase 3. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] |

## Project Constraints (from AGENTS.md)

- Read `radio-transcription/AGENTS.md` and `radio-transcription/.agents/instructions.md` before repository work. [VERIFIED: radio-transcription/AGENTS.md; VERIFIED: radio-transcription/.agents/instructions.md]
- For docs-only changes, prefer `git diff --check` instead of Python tests unless requested. [VERIFIED: radio-transcription/AGENTS.md]
- Avoid broad local E2E, API, component, Docker, testcontainers, or full integration-stack commands unless explicitly approved. [VERIFIED: radio-transcription/AGENTS.md; VERIFIED: radio-transcription/.agents/instructions.md]
- Prefer `mise` for repository formatting, linting, generation, and Terraform quality tasks. [VERIFIED: radio-transcription/.agents/instructions.md; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/.github/workflows/terraform-lint.yml]
- Use the `ctx7` CLI for current library, framework, SDK, API, CLI, or cloud-service docs. [VERIFIED: user-provided AGENTS.md instructions; VERIFIED: Context7 CLI docs lookups performed for `/hashicorp/terraform-provider-google` and `/websites/cloud_google_sdk`]
- Use `safe-run -- <command>` for agent-run tests, builds, installs, browser/e2e runs, benchmarks, and other resource-heavy commands. [VERIFIED: user-provided AGENTS.md instructions]
- Project-local `.codex/skills/` and `.agents/skills/` directories were not present in the active product repo, so no project skill `SKILL.md` rules applied. [VERIFIED: `find radio-transcription/.codex/skills radio-transcription/.agents/skills -maxdepth 2 -type f`]

## Standard Stack

### Core

| Library / Resource | Version | Purpose | Why Standard |
|--------------------|---------|---------|--------------|
| Terraform CLI | Installed 1.15.0; deployment modules require `>= 1.3` | Defines and validates GCP infrastructure resources. | Existing deployment repo uses Terraform environment roots and modules for Pub/Sub, Cloud Run, IAM, and logging resources. [VERIFIED: `terraform version`; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/versions.tf] |
| Terraform Google provider | Deployment pinned 7.21.0; latest observed 7.38.0 on GitHub releases | Provides `google_logging_project_sink`, `google_pubsub_topic`, `google_pubsub_subscription`, topic IAM, and Cloud Run IAM resources. | Deployment repo is pinned to 7.21.0, which already supports the needed resources; do not upgrade provider just for Phase 2. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/versions.tf; CITED: https://github.com/hashicorp/terraform-provider-google/releases] |
| `google_logging_project_sink` | Terraform Google provider 7.21.0 in deployment | Project-level Cloud Logging sink to Pub/Sub topic. | Official provider docs support Pub/Sub destinations, `filter`, `unique_writer_identity`, and computed `writer_identity`. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown] |
| `google_pubsub_topic` and `google_pubsub_subscription` | Terraform Google provider 7.21.0 in deployment | Dedicated notification topic, DLQ topic, retention subscription, and push subscription. | Existing deployment repo already uses these resources for pipeline topics and push subscriptions. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf] |
| `google_pubsub_topic_iam_member` | Terraform Google provider 7.21.0 in deployment | Non-authoritative topic IAM for sink publisher and DLQ publisher grants. | Provider docs define `*_iam_member` as non-authoritative and therefore safer for additive least-privilege grants. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_topic_iam.html.markdown] |
| `google_cloud_run_v2_service_iam_member` | Terraform Google provider 7.21.0 in deployment | Grants dedicated push invoker service account `roles/run.invoker` on the relay service. | Existing deployment modules use service-level Cloud Run IAM members for least-privilege invocation. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/cloud_run_v2_service_iam.html.markdown; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf] |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| Google Cloud SDK / `gcloud` | 565.0.0 installed | Post-deploy inspection of sinks, topic IAM, subscription push config, and Cloud Run IAM. | Use for ROUTE-01..04 verification after deployment or against an existing environment. [VERIFIED: `gcloud --version`; CITED: https://docs.cloud.google.com/sdk/gcloud/reference/logging] |
| `jq` | 1.7 installed | Inspect JSON output from `gcloud` commands. | Use for exact assertions in verification scripts and runbooks. [VERIFIED: `jq --version`] |
| `mise` | 2026.3.18 installed | Runs deployment repo validation tasks such as schema flattening and quality checks. | Existing Terraform CI uses `mise run flatten-schemas` and `mise run check`. [VERIFIED: `mise --version`; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/.github/workflows/terraform-lint.yml] |
| Context7 CLI / `ctx7` | Fetched through `npx --yes ctx7@latest` | Current Terraform provider and Google Cloud SDK docs lookup. | Required by project instructions for cloud service and provider documentation. [VERIFIED: Context7 CLI command output] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Deployment-owned route module | Public reusable Terraform module in `radio-transcription` | The concrete route depends on private deployment modules, relay service name/URL, and environment composition; a public module would either be too generic to help or would encode private topology. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/main.tf] |
| Log Router sink to Pub/Sub | Direct webhook call from producer code | Direct calls are explicitly out of scope and would couple feed writes to downstream latency and availability. [VERIFIED: .planning/REQUIREMENTS.md] |
| Pub/Sub retry/DLQ | Custom delivery table/outbox | A custom delivery table is out of scope for v1 and duplicates responsibility already covered by Pub/Sub DLQ and the audit ledger. [VERIFIED: .planning/REQUIREMENTS.md] |
| Dedicated push invoker service account | Reuse relay runtime service account | User decision D-09 rejects reusing application runtime accounts for Pub/Sub push authentication. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md] |

**Installation:**
```bash
# No new runtime packages are required for Phase 2. [VERIFIED: deployment Terraform/code inspection]
```

**Version verification:** Terraform CLI, Google provider pin, `gcloud`, `jq`, `mise`, and Context7 CLI availability were verified in this session. [VERIFIED: `terraform version`; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/versions.tf; VERIFIED: `gcloud --version`; VERIFIED: `jq --version`; VERIFIED: `mise --version`; VERIFIED: Context7 CLI output]

## Architecture Patterns

### System Architecture Diagram

```text
Phase 1 producers
  -> Python structured log extra={"json_fields": {event_type, schema_version, ...}}
  -> Cloud Logging receives LogEntry
  -> google_logging_project_sink filter:
       jsonPayload.event_type="radio_transcription.feed_audit_notification"
       AND jsonPayload.schema_version=1
  -> dedicated Pub/Sub topic
  -> push subscription with OIDC token
       - push endpoint: <relay_service_url>/pubsub/feed-audit-notifications
       - OIDC token service account: dedicated feed-audit push invoker
       - OIDC audience: <relay_service_url>
       - retry: 10s min, 60s max
       - DLQ after 10 delivery attempts
  -> Phase 3 relay Cloud Run service
  -> Watch Duty webhook

Failure branch:
  push endpoint non-2xx / timeout
  -> Pub/Sub retry policy
  -> dead_letter_policy
  -> dedicated DLQ topic
  -> DLQ retention subscription
```

The diagram reflects locked route decisions and official Pub/Sub/Logging behavior. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions]

### Recommended Project Structure

```text
radio-transcription-deployment/
  terraform/modules/message_queues/
    main.tf       # add feed-audit notification topic, DLQ topic, DLQ retention subscription [VERIFIED: existing module]
    outputs.tf    # add topic/DLQ id and name outputs [VERIFIED: existing module]
  terraform/modules/feed_audit_notification_route/
    main.tf       # new sink, sink IAM, push subscription, push invoker IAM [VERIFIED: recommended from context]
    variables.tf  # project, region, environment, topic ids/names, relay URL/name [VERIFIED: recommended from context]
    outputs.tf    # sink name, writer identity, subscription name, push invoker email [VERIFIED: recommended from context]
    versions.tf   # google provider >= 7.21.0 to match deployment modules [VERIFIED: deployment versions.tf]
  terraform/modules/app/
    main.tf       # call route module from app composition when relay inputs exist [VERIFIED: context D-03]
    variables.tf  # relay input variables only if Phase 2 must expose a contract before Phase 3 [VERIFIED: context D-07]
```

Do not add a public `radio-transcription/terraform/modules/...` module for this phase unless the planner identifies a reusable contract that does not know about deployment repo topology. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

### Pattern 1: Log Router Sink To Dedicated Topic

**What:** Create a project-level Logging sink with a Pub/Sub destination and a unique writer identity. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown]

**When to use:** Use for ROUTE-01 and ROUTE-02 in the deployment repo route module. [VERIFIED: .planning/REQUIREMENTS.md]

**Example:**
```hcl
# Source: Terraform Google provider logging_project_sink docs and Phase 2 CONTEXT.md.
resource "google_logging_project_sink" "feed_audit_notification" {
  project = var.project_id
  name    = "feed-audit-notification-route-${var.environment}"

  destination = "pubsub.googleapis.com/${var.notification_topic_id}"
  filter      = "jsonPayload.event_type=\"radio_transcription.feed_audit_notification\" AND jsonPayload.schema_version=1"

  unique_writer_identity = true
}

resource "google_pubsub_topic_iam_member" "sink_publisher" {
  project = var.project_id
  topic   = var.notification_topic_name
  role    = "roles/pubsub.publisher"
  member  = google_logging_project_sink.feed_audit_notification.writer_identity
}
```

Use `var.notification_topic_id` in `projects/<project>/topics/<topic>` form or build the full destination string from `project_id` and topic name; do not pass a bare topic name to the sink destination. [CITED: https://docs.cloud.google.com/logging/docs/export/configure_export_v2; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown]

### Pattern 2: Authenticated Pub/Sub Push To Cloud Run

**What:** Create a dedicated push-invoker service account, grant it `roles/run.invoker` on the relay Cloud Run service, and configure subscription OIDC with that service account. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions]

**When to use:** Use when Phase 2 wires the route to the Phase 3 relay endpoint. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

**Example:**
```hcl
# Source: existing transcription/normalization modules and Pub/Sub auth docs.
locals {
  relay_push_endpoint = "${trimsuffix(var.relay_service_url, "/")}/pubsub/feed-audit-notifications"
}

resource "google_service_account" "push_invoker" {
  account_id   = "feed-audit-push-invoker-${var.environment}"
  display_name = "Feed Audit Notification Pub/Sub Push Invoker"
}

resource "google_cloud_run_v2_service_iam_member" "push_invoker_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = var.relay_service_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.push_invoker.email}"
}

resource "google_pubsub_subscription" "feed_audit_notification_push" {
  project = var.project_id
  name    = "feed-audit-notification-subscription-${var.environment}"
  topic   = var.notification_topic_id

  push_config {
    push_endpoint = local.relay_push_endpoint

    oidc_token {
      service_account_email = google_service_account.push_invoker.email
      audience              = var.relay_service_url
    }
  }

  ack_deadline_seconds = 10

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }

  dead_letter_policy {
    dead_letter_topic     = var.dead_letter_topic_id
    max_delivery_attempts = 10
  }

  depends_on = [
    google_cloud_run_v2_service_iam_member.push_invoker_run_invoker,
    google_project_iam_member.pubsub_token_creator,
    google_pubsub_topic_iam_member.dlq_publisher,
  ]
}
```

Set `oidc_token.audience = var.relay_service_url` when `push_endpoint` includes `/pubsub/feed-audit-notifications`; Terraform defaults the audience to the full push endpoint if omitted, while Cloud Run service-to-service docs use the Cloud Run service URL as the ID-token audience. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service]

The example uses Terraform `trimsuffix` to remove one trailing slash from the relay service URL before appending the relay path. [CITED: https://developer.hashicorp.com/terraform/language/functions/trimsuffix]

### Pattern 3: Pub/Sub Service Agent IAM For OIDC And DLQ

**What:** Grant the Pub/Sub service agent token-creation permission for authenticated push and DLQ permissions for failed delivery handling. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions]

**When to use:** Use in the route module alongside the push subscription. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf]

**Example:**
```hcl
# Source: existing transcription/normalization modules and Pub/Sub REST docs.
data "google_project" "project" {
  project_id = var.project_id
}

locals {
  pubsub_service_agent = "service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "pubsub_token_creator" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:${local.pubsub_service_agent}"
}

resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  project = var.project_id
  topic   = var.dead_letter_topic_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${local.pubsub_service_agent}"
}

resource "google_pubsub_subscription_iam_member" "source_subscription_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.feed_audit_notification_push.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${local.pubsub_service_agent}"
}
```

The Pub/Sub service agent must publish to the DLQ topic, and a DLQ topic should have a subscription attached so messages are not lost. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf]

### Anti-Patterns to Avoid

- **Filtering by emitter service/resource:** Do not add Cloud Run names, VM names, resource types, or environment-specific producers to the sink filter; the locked boundary is the event contract. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]
- **Project-wide sink publisher:** Do not grant `roles/pubsub.publisher` at project level to the sink writer when topic-level IAM is available. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_topic_iam.html.markdown]
- **Default OIDC audience with path endpoints:** Do not rely on the default audience when push endpoint includes the relay path; set the audience to the Cloud Run service URL. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service]
- **DLQ topic without a retention subscription:** Do not create a DLQ topic without a subscription attached; Pub/Sub docs state messages published to topics without subscriptions are lost. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf]
- **Phase 2 placeholder relay:** Do not create a dummy Cloud Run service in Phase 2; the route should consume relay inputs and leave relay implementation to Phase 3. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Log routing from structured producer logs | Application Pub/Sub publisher in feed writes | Cloud Logging Log Router sink | Producer-side delivery clients are out of scope and would couple feed writes to notification delivery. [VERIFIED: .planning/REQUIREMENTS.md] |
| Redelivery backoff | Custom retry loop in route infrastructure | Pub/Sub `retry_policy` | Pub/Sub has first-class minimum and maximum backoff fields. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions] |
| Dead-letter handling | Custom failed-message table | Pub/Sub `dead_letter_policy` plus DLQ topic/subscription | Pub/Sub has bounded delivery attempts and DLQ routing; custom tables are out of scope. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; VERIFIED: .planning/REQUIREMENTS.md] |
| Push authentication | Custom bearer token or app-level shared secret for Pub/Sub-to-relay | Pub/Sub OIDC token + Cloud Run IAM | Pub/Sub supports authenticated push with signed JWTs, and Cloud Run authorizes invokers with IAM. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service] |
| IAM policy replacement | Authoritative topic/service IAM policies for one grant | `google_pubsub_topic_iam_member` and `google_cloud_run_v2_service_iam_member` | Non-authoritative members avoid fighting other IAM resources. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_topic_iam.html.markdown; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/cloud_run_v2_service_iam.html.markdown] |

**Key insight:** Phase 2 should compose managed GCP routing primitives, because the hard parts are IAM boundaries, message envelope shape, and retry/DLQ semantics already owned by Cloud Logging, Pub/Sub, and Cloud Run. [CITED: https://docs.cloud.google.com/logging/docs/export/pubsub; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; VERIFIED: .planning/REQUIREMENTS.md]

## Common Pitfalls

### Pitfall 1: Sink Writer IAM Missing Or Too Broad

**What goes wrong:** The sink exists but cannot publish, or the sink writer receives project-wide Pub/Sub publisher permissions. [CITED: https://docs.cloud.google.com/logging/docs/export/configure_export_v2; VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

**Why it happens:** Logging sinks expose a writer identity only after the sink exists, and the destination must grant that identity write permission. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown; CITED: https://docs.cloud.google.com/logging/docs/export/configure_export_v2]

**How to avoid:** Create the sink with `unique_writer_identity = true`, then grant `roles/pubsub.publisher` on only the notification topic through `google_pubsub_topic_iam_member`. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_topic_iam.html.markdown]

**Warning signs:** `gcloud logging sinks describe` shows a writer identity that is absent from `gcloud pubsub topics get-iam-policy` for the notification topic. [VERIFIED: gcloud command surface from Context7 `/websites/cloud_google_sdk`; CITED: https://docs.cloud.google.com/sdk/gcloud/reference/logging]

### Pitfall 2: Treating Routed Log Messages As Flat Payloads

**What goes wrong:** The relay expects the flat notification payload directly but receives a Pub/Sub wrapper whose `message.data` is a base64-encoded Cloud Logging `LogEntry`. [CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]

**Why it happens:** Cloud Logging routes complete log entries to Pub/Sub, not just `jsonPayload`. [CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]

**How to avoid:** Phase 2 should only configure routing; Phase 3 relay must decode the Pub/Sub message data as a `LogEntry` and then extract `jsonPayload`. [VERIFIED: .planning/ROADMAP.md; CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]

**Warning signs:** Verification only checks topic delivery but never documents the Cloud Logging `LogEntry` envelope. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

### Pitfall 3: OIDC Audience Mismatch For Path-Based Push Endpoint

**What goes wrong:** Pub/Sub push receives 401/403 from Cloud Run even though `oidc_token.service_account_email` and `roles/run.invoker` look correct. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service]

**Why it happens:** Terraform defaults OIDC audience to the push endpoint URL, but Cloud Run service-to-service auth expects the service URL audience unless custom audiences are configured. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service]

**How to avoid:** If push endpoint is `<service_url>/pubsub/feed-audit-notifications`, set `oidc_token.audience = var.relay_service_url`. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service]

**Warning signs:** Existing deployment examples omit `audience` because their push endpoints are bare service URIs. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/normalization/main.tf]

### Pitfall 4: Missing Pub/Sub Service Agent Permissions

**What goes wrong:** Authenticated push or dead-lettering fails after apply. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions]

**Why it happens:** Pub/Sub service agent needs Token Creator for OIDC signing, publisher on the DLQ topic, and acknowledge/subscriber permission on the source subscription for DLQ handling. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf]

**How to avoid:** Follow the transcription/normalization module pattern for `roles/iam.serviceAccountTokenCreator`, DLQ `roles/pubsub.publisher`, and subscription `roles/pubsub.subscriber`. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/normalization/main.tf]

**Warning signs:** Terraform has a push subscription with `oidc_token`, but no IAM grant for `service-${project_number}@gcp-sa-pubsub.iam.gserviceaccount.com`. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions]

### Pitfall 5: Making Environment Roots Fat

**What goes wrong:** Dev and prod roots diverge with duplicate route resources and variable plumbing. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/dev/main.tf; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/main.tf]

**Why it happens:** Route resources are added directly to env roots instead of the app composition/module layer. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

**How to avoid:** Add resources to `message_queues` and a small route module, then call from `modules/app`. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/main.tf; VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

**Warning signs:** The same sink/subscription HCL appears separately under `terraform/environments/dev` and `terraform/environments/prod`. [VERIFIED: deployment repo structure inspection]

## Code Examples

### Message Queue Topic And DLQ Additions

```hcl
# Source: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf
resource "google_pubsub_topic" "feed_audit_notification" {
  name = "feed-audit-notification-${var.environment}"
}

resource "google_pubsub_topic" "feed_audit_notification_dlq" {
  name = "feed-audit-notification-dlq-${var.environment}"
}

resource "google_pubsub_subscription" "feed_audit_notification_dlq_subscription" {
  name  = "feed-audit-notification-dlq-subscription-${var.environment}"
  topic = google_pubsub_topic.feed_audit_notification_dlq.name

  message_retention_duration = "604800s"
}
```

The DLQ retention subscription follows the existing message queue module pattern and Pub/Sub docs warning that topics without subscriptions lose messages. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions]

### Verification Commands After Deploy

```bash
# Source: gcloud SDK docs and Phase 2 requirements.
gcloud logging sinks describe "feed-audit-notification-route-${ENV}" \
  --project "${PROJECT_ID}" \
  --format='json(name,destination,filter,writerIdentity)'

gcloud pubsub topics get-iam-policy "feed-audit-notification-${ENV}" \
  --project "${PROJECT_ID}" \
  --format=json

gcloud pubsub subscriptions describe "feed-audit-notification-subscription-${ENV}" \
  --project "${PROJECT_ID}" \
  --format=json

gcloud run services get-iam-policy "${RELAY_SERVICE_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --format=json
```

These checks prove sink filter/destination/writer identity, topic-level publisher IAM, push OIDC settings, retry/DLQ settings, and Cloud Run invoker IAM after resources exist. [VERIFIED: Context7 `/websites/cloud_google_sdk`; CITED: https://docs.cloud.google.com/sdk/gcloud/reference/logging; CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions]

### Static Checks Without A Deployed Relay

```bash
# Source: deployment repo Terraform CI pattern.
terraform -chdir=terraform/environments/dev init -backend=false
mise run flatten-schemas
terraform -chdir=terraform/environments/dev validate
terraform -chdir=terraform/environments/prod init -backend=false
terraform -chdir=terraform/environments/prod validate
mise run check

rg -n 'jsonPayload\.event_type="radio_transcription\.feed_audit_notification"|jsonPayload\.schema_version=1|roles/pubsub.publisher|minimum_backoff = "10s"|maximum_backoff = "60s"|max_delivery_attempts = 10|oidc_token|roles/run.invoker' terraform
```

These static checks can validate HCL shape and repository conventions before the Phase 3 relay implementation exists; they do not prove end-to-end delivery. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/.github/workflows/terraform-lint.yml; VERIFIED: .planning/ROADMAP.md]

## State Of The Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Direct notification delivery from feed mutation paths | Structured log emission plus Cloud Logging sink to Pub/Sub | Locked in this milestone on 2026-06-26 | Keeps feed lifecycle and ingestion writes independent from notification delivery latency/failure. [VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/ROADMAP.md] |
| Pub/Sub push without dedicated invoker identity | Dedicated push invoker service account per service/route | Existing deployment modules at commit `14ac7c4` | Keeps Cloud Run invocation IAM separate from runtime service accounts. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf; VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md] |
| Default push endpoint as OIDC audience | Explicit service URL audience when push endpoint includes a path | Required by Phase 3 endpoint path design | Avoids Cloud Run audience mismatch when the push URL is not exactly the service URL. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service] |

**Deprecated/outdated:**
- Creating route resources directly in each env root is outdated for this deployment repo because dev/prod roots are thin wrappers over `module "app"`. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/dev/main.tf; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/main.tf]
- Reusing notification runtime service account as Pub/Sub push auth identity is not acceptable for Phase 2 because D-09 requires a dedicated push invoker service account. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The Phase 3 relay Cloud Run service will expose a stable base service URL and service name that Phase 2 route resources can consume. [ASSUMED] | Architecture Patterns / Open Questions | If Phase 3 chooses a different deployment shape, the route module inputs must change before app integration. |
| A2 | Phase 2 can merge a route module and app input contract before requiring deploy-time relay values. [ASSUMED] | Open Questions | If deployment policy requires every merged app module to be immediately applyable, Phase 2 must land with Phase 3 or use a reviewed feature gate. |
| A3 | Research validity windows of 30 days for deployment patterns and 7 days for provider/GCP docs are sufficient. [ASSUMED] | Metadata | If provider or GCP IAM behavior changes sooner, planner should re-run docs lookup before implementation. |

## Open Questions (RESOLVED)

1. **Should Phase 2 merge app-module instantiation before the relay module exists?**
   - What we know: Phase 2 must define the full route shape with relay inputs and no placeholder relay. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md]
   - What's unclear: A concrete `module "feed_audit_notification_route"` call in `modules/app` needs relay service URL/name values that Phase 3 has not created yet. [VERIFIED: .planning/ROADMAP.md]
   - Resolution: Implement the route module and app-module wiring in Phase 2 behind `feed_audit_notification_route_enabled = false` by default, with nullable relay URL/name variables. This makes the app module validate and apply before the Phase 3 relay exists. Phase 3 supplies the real relay outputs and flips the enable flag when the relay can receive Pub/Sub push. [VERIFIED: deployment repo module structure; ASSUMED]

2. **Does the WIF Terraform deployer already have `iam.serviceAccounts.actAs` on push auth service accounts?**
   - What we know: Pub/Sub docs say the subscription creator/updater must have `iam.serviceAccounts.actAs` on the push auth service account. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions]
   - What's unclear: The deployment repo may grant this outside Terraform, because existing push subscriptions already use OIDC. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/.github/workflows/terraform_deploy.yml]
   - Resolution: The route module should own an explicit least-privilege deployer grant for the new push invoker service account. Add `deployer_service_account_email` as a route input, create `google_service_account_iam_member.deployer_push_invoker_user` on `google_service_account.push_invoker.name` with `roles/iam.serviceAccountUser`, and have the app module pass `local.deployer_sa_email` derived from the existing WIF deployer configuration. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/main.tf]

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| `/tmp/radio-transcription-deployment-main-14ac7c4` | Existing deployment patterns | yes | commit `14ac7c4` | None; user explicitly required this worktree. [VERIFIED: `git -C /tmp/radio-transcription-deployment-main-14ac7c4 rev-parse --short HEAD`] |
| Terraform CLI | Terraform validation/planning | yes | 1.15.0 | Use CI if local provider init is unavailable. [VERIFIED: `terraform version`] |
| Terraform Google provider | GCP resources | yes in config | pinned 7.21.0 | Do not upgrade for Phase 2 unless provider validation proves a missing field. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/versions.tf] |
| Google Cloud SDK | Post-deploy verification | yes | 565.0.0 | Use Terraform state/plan JSON for static checks before deploy. [VERIFIED: `gcloud --version`] |
| `jq` | JSON assertion commands | yes | 1.7 | Use `--format='value(...)'` for simpler gcloud checks. [VERIFIED: `jq --version`] |
| `mise` | Deployment quality tasks | yes | 2026.3.18 | Run underlying `terraform` commands directly if mise is unavailable. [VERIFIED: `mise --version`] |
| `safe-run` | Host-stable local validation wrapper | yes | path available | Use only for potentially heavy tests/builds; Terraform validate is lightweight but can still be wrapped. [VERIFIED: `command -v safe-run`; VERIFIED: user-provided AGENTS.md instructions] |
| Phase 3 relay service | Concrete push endpoint and Cloud Run IAM binding | no | not implemented | Use relay input contract and static HCL validation until Phase 3. [VERIFIED: .planning/ROADMAP.md] |

**Missing dependencies with no fallback:**
- A deployed relay service is missing for runtime push verification; Phase 2 can still validate Terraform shape and IAM intent without deploying the relay. [VERIFIED: .planning/ROADMAP.md]

**Missing dependencies with fallback:**
- None for research and static planning. [VERIFIED: environment audit commands]

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | yes | Pub/Sub authenticated push OIDC token generated for a dedicated service account. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions] |
| V3 Session Management | no | No browser/user session is introduced in Phase 2. [VERIFIED: .planning/ROADMAP.md] |
| V4 Access Control | yes | Topic-level sink publisher IAM and service-level Cloud Run invoker IAM. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_topic_iam.html.markdown; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/cloud_run_v2_service_iam.html.markdown] |
| V5 Input Validation | yes | Sink filter accepts only the locked `event_type` and `schema_version` contract; Phase 3 validates message payload. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; VERIFIED: .planning/ROADMAP.md] |
| V6 Cryptography | yes | Use Google-managed OIDC/JWT signing and Cloud Run IAM; do not hand-roll signing for Pub/Sub push. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service] |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Spoofed direct relay calls | Spoofing | Cloud Run requires authenticated invoker identity; Pub/Sub push uses dedicated OIDC service account. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service] |
| Overbroad sink publishing | Elevation of privilege | Grant sink writer `roles/pubsub.publisher` only on the notification topic. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md] |
| Unbounded redelivery storm | Denial of service | Configure retry backoff 10s/60s and DLQ after 10 attempts. [VERIFIED: .planning/REQUIREMENTS.md; CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions] |
| Message loss from DLQ topic without subscribers | Tampering / Availability | Create a DLQ retention subscription in `message_queues`. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf] |
| Audience mismatch bypassing expected auth path | Spoofing / Denial of service | Set OIDC audience to relay service URL when push endpoint includes a path. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service] |

## Sources

### Primary (HIGH confidence)
- Context7 `/hashicorp/terraform-provider-google` - `google_pubsub_subscription`, `push_config`, `oidc_token`, `dead_letter_policy`, `retry_policy`. [VERIFIED: Context7 CLI output]
- Context7 `/websites/cloud_google_sdk` - `gcloud logging`, Pub/Sub IAM command groups, push auth CLI flags. [VERIFIED: Context7 CLI output]
- Google Cloud Logging Pub/Sub export docs - Pub/Sub message shape and near-real-time routed log behavior. [CITED: https://docs.cloud.google.com/logging/docs/export/pubsub]
- Google Cloud Logging route sink docs - destination formats, sink writer identity, and destination permissions. [CITED: https://docs.cloud.google.com/logging/docs/export/configure_export_v2]
- Google Pub/Sub subscriptions REST docs - `pushConfig`, `deadLetterPolicy`, `retryPolicy`, max attempts, backoff bounds, and DLQ service-agent requirements. [CITED: https://docs.cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions]
- Google Pub/Sub authenticated push docs - OIDC token behavior, Token Creator requirement, and ActAs requirement. [CITED: https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions]
- Cloud Run service-to-service auth docs - ID-token audience and bearer-token invocation model. [CITED: https://docs.cloud.google.com/run/docs/authenticating/service-to-service]
- Terraform `trimsuffix` function docs - relay URL normalization in the HCL example. [CITED: https://developer.hashicorp.com/terraform/language/functions/trimsuffix]
- Terraform Google provider docs - `google_logging_project_sink`, `google_pubsub_subscription`, `google_pubsub_topic_iam_member`, and `google_cloud_run_v2_service_iam_member`. [CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/logging_project_sink.html.markdown; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_subscription.html.markdown; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/pubsub_topic_iam.html.markdown; CITED: https://raw.githubusercontent.com/hashicorp/terraform-provider-google/main/website/docs/r/cloud_run_v2_service_iam.html.markdown]
- Phase 2 context and requirements. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; VERIFIED: .planning/REQUIREMENTS.md; VERIFIED: .planning/ROADMAP.md]
- Deployment repo commit `14ac7c4` Terraform modules. [VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4]

### Secondary (MEDIUM confidence)
- Terraform Google provider latest release observed as v7.38.0 on GitHub releases; deployment remains pinned to 7.21.0. [CITED: https://github.com/hashicorp/terraform-provider-google/releases; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/versions.tf]

### Tertiary (LOW confidence)
- None. [VERIFIED: all non-assumption claims in this research cite local artifacts, Context7 output, or official docs]

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - resource types and fields are verified against Terraform provider docs, official GCP docs, and existing deployment modules. [CITED: Terraform provider docs above; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4]
- Architecture: HIGH - route ownership, filter boundary, and relay separation are locked in Phase 2 context and roadmap. [VERIFIED: .planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md; VERIFIED: .planning/ROADMAP.md]
- Pitfalls: HIGH - sink writer IAM, Pub/Sub service-agent IAM, DLQ subscription, and OIDC audience behavior are documented by official GCP/provider docs and visible deployment patterns. [CITED: GCP/provider docs above; VERIFIED: /tmp/radio-transcription-deployment-main-14ac7c4]
- App integration timing: MEDIUM - relay resources are deferred to Phase 3, so Phase 2 needs a contract or same-branch sequencing decision. [VERIFIED: .planning/ROADMAP.md; ASSUMED]

**Research date:** 2026-06-26 [VERIFIED: environment current date]
**Valid until:** 2026-07-26 for deployment repo patterns; 2026-07-03 for provider/GCP docs if planning changes IAM or Pub/Sub push details. [ASSUMED]
