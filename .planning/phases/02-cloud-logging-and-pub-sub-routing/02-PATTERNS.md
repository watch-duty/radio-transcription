# Phase 2: Cloud Logging and Pub/Sub Routing - Pattern Map

**Mapped:** 2026-06-26
**Files analyzed:** 9 new/modified files
**Analogs found:** 9 / 9

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `radio-transcription-deployment/terraform/modules/message_queues/main.tf` | config | pub-sub | `terraform/modules/message_queues/main.tf` | exact |
| `radio-transcription-deployment/terraform/modules/message_queues/outputs.tf` | config | pub-sub | `terraform/modules/message_queues/outputs.tf` | exact |
| `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/main.tf` | config | pub-sub + request-response + event-driven | `terraform/modules/services/transcription/main.tf` + `terraform/modules/services/notification/main.tf` | role-match |
| `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/variables.tf` | config | request-response | `terraform/modules/services/transcription/variables.tf` | role-match |
| `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/outputs.tf` | config | pub-sub + request-response | `terraform/modules/services/notification/outputs.tf` | role-match |
| `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/versions.tf` | config | Terraform provider config | `terraform/modules/message_queues/versions.tf` | exact |
| `radio-transcription-deployment/terraform/modules/app/main.tf` | config | pub-sub + request-response | `terraform/modules/app/main.tf` | exact |
| `radio-transcription-deployment/terraform/modules/app/variables.tf` | config | request-response | `terraform/modules/app/variables.tf` | exact |
| `radio-transcription-deployment/terraform/modules/app/outputs.tf` | config | pub-sub + request-response | `terraform/modules/app/outputs.tf` | exact |

## Pattern Assignments

### `radio-transcription-deployment/terraform/modules/message_queues/main.tf` (config, pub-sub)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/main.tf`

**Module header and sectioning pattern** (lines 1-3, 85-87):
```hcl
# =============================================================================
# MESSAGE QUEUES MODULE
# =============================================================================

# =============================================================================
# PUBSUB PIPELINE QUEUES
# =============================================================================
```

**Topic resource pattern** (lines 89-97):
```hcl
# Topic for incoming raw audio transcriptions
resource "google_pubsub_topic" "transcribed_audio" {
  name = "transcribed-audio-${var.environment}"

  schema_settings {
    schema   = google_pubsub_schema.transcribed_audio_schema.id
    encoding = "BINARY"
  }
}
```

**DLQ topic plus retention subscription pattern** (lines 149-165):
```hcl
# =============================================================================
# PUBSUB DEAD LETTER QUEUES
# Any message sent to a DLQ with no subscription attached is permanently lost
# =============================================================================

# Dead letter topic for evaluated audio processing failures
resource "google_pubsub_topic" "evaluated_audio_dead_letter" {
  name = "evaluated-audio-dead-letter-${var.environment}"
}

# Subscription to the DLQ for evaluated audio
resource "google_pubsub_subscription" "evaluated_audio_dead_letter_subscription" {
  name  = "evaluated-audio-dead-letter-subscription-${var.environment}"
  topic = google_pubsub_topic.evaluated_audio_dead_letter.name

  message_retention_duration = "604800s" # 7 days
}
```

**Apply:** Add a schema-less `google_pubsub_topic` for feed-audit notifications and a schema-less DLQ topic plus DLQ retention subscription. Keep names domain-specific, for example `feed-audit-notification-${var.environment}` and `feed-audit-notification-dlq-${var.environment}`. Do not add protobuf schema resources for routed Cloud Logging `LogEntry` envelopes.

---

### `radio-transcription-deployment/terraform/modules/message_queues/outputs.tf` (config, pub-sub)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/outputs.tf`

**Topic ID/name output pattern** (lines 1-9):
```hcl
output "topic_transcribed_audio_id" {
  description = "The ID of the transcribed audio topic."
  value       = google_pubsub_topic.transcribed_audio.id
}

output "topic_transcribed_audio_name" {
  description = "The name of the transcribed audio topic."
  value       = google_pubsub_topic.transcribed_audio.name
}
```

**DLQ output pattern** (lines 16-23):
```hcl
output "topic_evaluated_audio_dead_letter_id" {
  description = "The ID of the evaluated audio dead letter topic."
  value       = google_pubsub_topic.evaluated_audio_dead_letter.id
}

output "topic_evaluated_audio_dead_letter_name" {
  description = "The name of the evaluated audio dead letter topic."
  value       = google_pubsub_topic.evaluated_audio_dead_letter.name
}
```

**Apply:** Add both ID and name outputs for the notification topic and DLQ topic. The route module needs topic IDs for sink/subscription resource references and topic names for IAM resources that expect names.

---

### `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/main.tf` (config, pub-sub + request-response + event-driven)

**Primary analogs:**
- `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf`
- `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/notification/main.tf`
- `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/monitoring.tf` for local Cloud Logging filter syntax

**Local filter style pattern** (`terraform/modules/app/monitoring.tf` lines 8-16):
```hcl
resource "google_logging_metric" "transcription_e2e_latency_ms" {
  project     = var.project_id
  name        = "transcription_e2e_latency_ms"
  description = "End-to-end processing latency from ingestion to evaluation."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND jsonPayload.event_type="e2e_latency"
    AND jsonPayload.latency_ms >= 0
  EOT
```

**Apply:** The sink filter should copy the heredoc style but use only the event contract:
```hcl
filter = <<-EOT
  jsonPayload.event_type="radio_transcription.feed_audit_notification"
  AND jsonPayload.schema_version=1
EOT
```

Do not add `resource.type`, service names, logger names, or environment-specific emitters.

**Authenticated push subscription pattern** (`services/notification/main.tf` lines 126-155):
```hcl
# Direct Pub/Sub push subscription from evaluated results to the notification pipeline
resource "google_pubsub_subscription" "alert_notification_pubsub_subscription" {
  name  = "alert-notification-subscription-${var.environment}"
  topic = var.topic_evaluated_audio_id

  enable_message_ordering = true

  push_config {
    push_endpoint = google_cloud_run_v2_service.notification_pipeline.uri

    oidc_token {
      service_account_email = google_service_account.notification_pipeline_sa.email
    }
  }

  ack_deadline_seconds = 10

  dead_letter_policy {
    dead_letter_topic     = var.topic_evaluated_audio_dead_letter_id
    max_delivery_attempts = 8
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  depends_on = [
    google_project_iam_member.notification_pipeline_roles
  ]
}
```

**Apply:** Copy the `push_config`, `ack_deadline_seconds`, `dead_letter_policy`, `retry_policy`, and `depends_on` shape, but set `minimum_backoff = "10s"`, `maximum_backoff = "60s"`, and `max_delivery_attempts = 10`.

**Dedicated push invoker service account pattern** (`services/transcription/main.tf` lines 160-177):
```hcl
# -----------------------------------------------------------------------------
# Pub/Sub Push Invoker IAM
# -----------------------------------------------------------------------------

# Service account for the Pub/Sub push invoker
resource "google_service_account" "pubsub_invoker" {
  account_id   = "transcribe-pubsub-invoker-${var.environment}"
  display_name = "Transcription Pub/Sub Push Invoker"
}

# Allow Pub/Sub invoker to call our Cloud Run service
resource "google_cloud_run_v2_service_iam_member" "pubsub_invoker_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.transcription_service.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}
```

**Apply:** Create a dedicated feed-audit push invoker service account in the route module. Grant it `roles/run.invoker` on `var.relay_service_name`, not on a service created in Phase 2.

**Explicit OIDC audience pattern** (`services/ingestion/broadcastify_credential_rotation/main.tf` lines 160-168):
```hcl
http_target {
  uri         = google_cloud_run_v2_service.broadcastify_credential_rotation.uri
  http_method = "POST"

  oidc_token {
    service_account_email = google_service_account.broadcastify_credential_rotation_sa.email
    audience              = google_cloud_run_v2_service.broadcastify_credential_rotation.uri
  }
}
```

**Apply:** Existing Pub/Sub push examples omit `audience` because their push endpoint is the bare Cloud Run URI. For this phase's path-based endpoint, set `oidc_token.audience = var.relay_service_url` while `push_endpoint` appends the relay path.

**Pub/Sub service-agent IAM pattern** (`services/transcription/main.tf` lines 179-202):
```hcl
data "google_project" "project" {}

# Allow Pub/Sub to publish failed messages to the Dead Letter Queue
resource "google_pubsub_topic_iam_member" "transcription_dlq_publisher" {
  project = var.project_id
  topic   = var.topic_transcribed_audio_dlq_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allows Pub/Sub SA to pull and acknowledge messages that will be put on the DLQ
resource "google_pubsub_subscription_iam_member" "transcription_pubsub_subscriber" {
  project      = var.project_id
  subscription = google_pubsub_subscription.transcription_pubsub_subscription.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allow Pub/Sub service to create identity tokens for push endpoints
resource "google_project_iam_member" "pubsub_token_creator" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
```

**Apply:** Reuse the project data source and Pub/Sub service-agent member string exactly. Add DLQ publisher, source subscription subscriber, and token creator grants.

**Sink writer publisher IAM pattern** (`services/transcription/main.tf` lines 181-187):
```hcl
resource "google_pubsub_topic_iam_member" "transcription_dlq_publisher" {
  project = var.project_id
  topic   = var.topic_transcribed_audio_dlq_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
```

**Apply:** Use the same non-authoritative `google_pubsub_topic_iam_member` resource type for the Logging sink writer, but `member` must be `google_logging_project_sink.feed_audit_notification.writer_identity`, and `topic` must be the notification topic name only.

**No exact local analog:** The deployment repo has no `google_logging_project_sink` resource. Use `02-RESEARCH.md` Pattern 1 for the resource block: `google_logging_project_sink`, Pub/Sub destination, `unique_writer_identity = true`, and topic-level publisher IAM.

---

### `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/variables.tf` (config, request-response)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/variables.tf`

**Common module variable pattern** (lines 1-14):
```hcl
variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  description = "GCP Region"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"
}
```

**Topic input pattern** (lines 16-29):
```hcl
variable "topic_normalized_audio_id" {
  type        = string
  description = "Topic ID for normalized audio."
}

variable "topic_transcribed_audio_id" {
  type        = string
  description = "Topic ID for transcribed audio output."
}

variable "topic_transcribed_audio_dlq_name" {
  type        = string
  description = "DLQ topic name for transcription failures."
}
```

**Cloud Run service reference pattern** (lines 55-62):
```hcl
variable "audio_segments_service_name" {
  description = "The name of the audio segments service (Cloud Run)."
  type        = string
}

variable "audio_segments_api_url" {
  description = "External API URL handling audio segments."
  type        = string
}
```

**Optional URL validation pattern** (`services/notification/variables.tf` lines 77-84):
```hcl
variable "webapp_base_url" {
  description = "The base URL of the webapp. Must be a fully qualified HTTPS URL (for example, https://example.com) and must not include a trailing slash."
  type        = string

  validation {
    condition     = can(regex("^https://[^/]+(?:/[^/].*)?$", var.webapp_base_url)) && !endswith(var.webapp_base_url, "/")
    error_message = "webapp_base_url must be a fully qualified HTTPS URL with a scheme (for example, https://example.com) and must not end with a trailing slash."
  }
}
```

**Apply:** Define `project_id`, `region`, `environment`, notification topic ID/name, DLQ topic ID/name, `relay_service_url`, and `relay_service_name`. If `relay_service_url` will be concatenated with a path, either validate no trailing slash or normalize with `trimsuffix` in `main.tf`.

---

### `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/outputs.tf` (config, pub-sub + request-response)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/notification/outputs.tf`

**Service/subscription output pattern** (lines 1-14):
```hcl
output "notification_service_name" {
  description = "The name of the deployed notification pipeline Cloud Run service."
  value       = google_cloud_run_v2_service.notification_pipeline.name
}

output "notification_service_uri" {
  description = "The URI of the deployed notification pipeline Cloud Run service."
  value       = google_cloud_run_v2_service.notification_pipeline.uri
}

output "alert_notification_subscription_name" {
  description = "The name of the Pub/Sub push subscription for notifications."
  value       = google_pubsub_subscription.alert_notification_pubsub_subscription.name
}
```

**Apply:** Output at least the Logging sink name, sink writer identity, push subscription name, and push invoker service account email. Keep descriptions specific enough for Phase 3 and verification plans to reference.

---

### `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/versions.tf` (config, Terraform provider config)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/message_queues/versions.tf`

**Provider constraint pattern** (lines 1-9):
```hcl
terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 7.21.0"
    }
  }
}
```

**Apply:** Copy this exactly unless a validation run proves a missing resource field. Do not upgrade the Google provider for Phase 2 by default.

---

### `radio-transcription-deployment/terraform/modules/app/main.tf` (config, pub-sub + request-response)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/main.tf`

**Top-down section layout** (lines 1-20):
```hcl
# =============================================================================
# RADIO TRANSCRIPTION INFRASTRUCTURE (APP MODULE)
# =============================================================================
#
# File layout:
# 1. Project Foundation
# 2. Networking
# 3. Secrets Management & Passwords
# 4. Database (AlloyDB / Memorystore for Redis)
# 5. Messaging (Pub/Sub)
# 6. Identity & Access Management (IAM)
# 7. Cloud Run Services
```

**Message queue module call pattern** (lines 102-110):
```hcl
# =============================================================================
# 5. MESSAGING (PUB/SUB)
# =============================================================================

module "message_queues" {
  source      = "../message_queues"
  depends_on  = [google_project_service.apis]
  environment = var.environment
}
```

**Service module wiring pattern** (lines 161-178):
```hcl
module "transcription" {
  source = "../services/transcription"

  project_id                       = var.project_id
  region                           = var.region
  environment                      = var.environment
  topic_normalized_audio_id        = module.message_queues.topic_normalized_audio_id
  topic_transcribed_audio_id       = module.message_queues.topic_transcribed_audio_id
  topic_transcribed_audio_dlq_name = module.message_queues.topic_transcribed_audio_dlq_name
  min_instances                    = var.transcription_min_instances
  max_instances                    = var.transcription_max_instances
  transcriber_type                 = var.transcriber_type
  transcriber_config               = var.transcriber_config
  audio_segments_service_name      = module.audio_segments_api.audio_segments_api_service_name
  audio_segments_api_url           = module.audio_segments_api.audio_segments_api_service_url

  depends_on = [google_project_service.apis]
}
```

**Notification module wiring pattern** (lines 207-232):
```hcl
module "notification" {
  source = "../services/notification"

  project_id                           = var.project_id
  region                               = var.region
  environment                          = var.environment
  network_name                         = module.network.network_name
  subnet_name                          = module.network.subnet_name
  wd_backend_endpoint                  = var.wd_backend_endpoint
  wd_backend_endpoint_api_key          = var.wd_backend_endpoint_api_key
  redis_host                           = module.storage.redis_host
  redis_port                           = module.storage.redis_port
  redis_password_secret_id             = module.storage.redis_password_secret_id
  redis_password_secret_full_id        = module.storage.redis_password_secret_full_id
  redis_certificate_secret_id          = module.storage.redis_certificate_secret_id
  redis_certificate_secret_full_id     = module.storage.redis_certificate_secret_full_id
  topic_evaluated_audio_id             = module.message_queues.topic_evaluated_audio_id
  topic_evaluated_audio_dead_letter_id = module.message_queues.topic_evaluated_audio_dead_letter_id
  webapp_base_url                      = var.web_domain
  feeds_api_url                        = module.feed_store.feed_store_service_url

  depends_on = [
    google_project_service.apis,
    module.storage
  ]
}
```

**Apply:** Add the route module call in the app module, preferably near Messaging or after the Phase 3 relay module if it exists by implementation time. Pass topic IDs/names from `module.message_queues` and relay service URL/name from the relay module or explicit app variables. Keep environment roots thin.

---

### `radio-transcription-deployment/terraform/modules/app/variables.tf` (config, request-response)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/variables.tf`

**Common app variable pattern** (lines 1-14):
```hcl
variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., prod, pre-prod, dev)."
  type        = string
}
```

**Deployment-provided variable pattern** (lines 21-33):
```hcl
# This is used by the notification service as the endpoint to send alerts to.
# It is provided by TF_VAR_wd_backend_endpoint in the deployment.
variable "wd_backend_endpoint" {
  type      = string
  sensitive = false
}

# This is used by the notification service as the API key to authenticate with the endpoint.
# It is provided by TF_VAR_wd_backend_endpoint_api_key in the deployment.
variable "wd_backend_endpoint_api_key" {
  type      = string
  sensitive = true # Prevents the value from appearing in logs
}
```

**Defaulted operational setting pattern** (lines 173-195):
```hcl
variable "transcription_min_instances" {
  description = "Minimum instances for transcription service."
  type        = number
  default     = 0
}

variable "transcription_max_instances" {
  description = "Maximum instances for transcription service."
  type        = number
  default     = 10
}

variable "transcriber_type" {
  description = "Type of transcription model to use."
  type        = string
  default     = "GOOGLE_CHIRP_V3"
}

variable "transcriber_config" {
  description = "JSON string of transcriber-specific configuration."
  type        = string
  default     = "{}"
}
```

**Apply:** Add relay input variables here only if the route module must be callable before the Phase 3 relay module exists. Prefer non-sensitive strings for service URL/name. If Phase 3 creates the relay module first or in the same branch, pass module outputs directly and avoid extra env-root variables.

---

### `radio-transcription-deployment/terraform/modules/app/outputs.tf` (config, pub-sub + request-response)

**Analog:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/app/outputs.tf`

**Topic output pattern** (lines 47-60):
```hcl
output "topic_transcribed_audio" {
  description = "The Pub/Sub topic that triggers the pipeline"
  value       = module.message_queues.topic_transcribed_audio_id
}

output "topic_evaluated_audio" {
  description = "The Pub/Sub topic where results are published"
  value       = module.message_queues.topic_evaluated_audio_id
}

output "topic_continuous_audio" {
  description = "The Pub/Sub topic for continuous audio available (GCS paths of audio to transcribe)."
  value       = module.message_queues.topic_continuous_audio_id
}
```

**Service output pattern** (lines 93-100):
```hcl
output "radio_transcription_api_url" {
  description = "The URL of the radio transcription API"
  value       = module.radio_transcription_api.radio_transcription_api_url
}

output "monitoring_log_metric_names" {
  description = "Log-based metric names from the monitoring sub-module (null when enable_monitoring=false)."
  value       = module.ingestion.monitoring_log_metric_names
}
```

**Apply:** Expose route outputs only if they are needed by environment roots, deploy workflows, or later verification. Good candidates are sink name, subscription name, and push invoker service account email. Do not expose secrets.

## Shared Patterns

### Thin Environment Roots

**Source:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/dev/main.tf` lines 1-40

```hcl
module "app" {
  source = "../../modules/app"

  project_id                      = var.project_id
  region                          = var.region
  environment                     = var.environment
  allow_production_deployment     = var.allow_production_deployment
  wd_backend_endpoint             = var.wd_backend_endpoint
  wd_backend_endpoint_api_key     = var.wd_backend_endpoint_api_key
  ingestion_staging_bucket_name   = var.ingestion_staging_bucket_name
  ingestion_canonical_bucket_name = var.ingestion_canonical_bucket_name
```

**Source:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/environments/prod/main.tf` lines 40-86

```hcl
module "app" {
  source = "../../modules/app"

  project_id                      = var.project_id
  region                          = var.region
  environment                     = var.environment
  allow_production_deployment     = var.allow_production_deployment
  wd_backend_endpoint             = var.wd_backend_endpoint
  wd_backend_endpoint_api_key     = var.wd_backend_endpoint_api_key
  ingestion_staging_bucket_name   = var.ingestion_staging_bucket_name
  ingestion_canonical_bucket_name = var.ingestion_canonical_bucket_name
```

**Apply to:** Route integration. Do not duplicate sink/subscription resources under both env roots. Only touch env roots if new app variables must be passed from environment-specific values.

### Non-Authoritative IAM Members

**Source:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/transcription/main.tf` lines 171-187

```hcl
resource "google_cloud_run_v2_service_iam_member" "pubsub_invoker_run_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.transcription_service.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}

data "google_project" "project" {}

# Allow Pub/Sub to publish failed messages to the Dead Letter Queue
resource "google_pubsub_topic_iam_member" "transcription_dlq_publisher" {
  project = var.project_id
  topic   = var.topic_transcribed_audio_dlq_name
  role    = "roles/pubsub.publisher"
```

**Apply to:** Sink publisher grant, DLQ publisher grant, source subscription subscriber grant, and Cloud Run invoker grant. Use `*_iam_member`, not authoritative IAM policy replacement.

### Pub/Sub Push Dependencies

**Source:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform/modules/services/normalization/main.tf` lines 70-80

```hcl
dead_letter_policy {
  dead_letter_topic     = "projects/${var.project_id}/topics/${var.topic_normalized_audio_dlq_name}"
  max_delivery_attempts = 5
}

depends_on = [
  google_project_iam_member.pubsub_token_creator,
  google_cloud_run_v2_service_iam_member.pubsub_invoker_run_invoker,
  google_pubsub_topic_iam_member.normalization_dlq_publisher
]
```

**Apply to:** Feed-audit route subscription. Include dependencies for token creator, Cloud Run invoker, sink publisher if needed by ordering, and DLQ IAM.

### Terraform CI Validation

**Source:** `/tmp/radio-transcription-deployment-main-14ac7c4/.github/workflows/terraform-lint.yml` lines 68-81

```yaml
- name: Terraform Init (No Backend)
  working-directory: terraform/environments/${{ matrix.environment }}
  run: terraform init -backend=false

- name: Prepare Pub/Sub Protobuf Schemas
  working-directory: terraform/environments/${{ matrix.environment }}
  run: mise run flatten-schemas

- name: Terraform Validate
  working-directory: terraform/environments/${{ matrix.environment }}
  run: terraform validate

- name: Run Quality Checks (Mise)
  run: mise run check
```

**Apply to:** Verification plans. For local agent-run validation, prefer `safe-run -- terraform -chdir=... validate` only after `terraform init -backend=false` has completed, and keep checks narrow.

## No Local Exact Analog

| Resource / Concern | Target File | Role | Data Flow | Reason |
|--------------------|-------------|------|-----------|--------|
| `google_logging_project_sink` with Pub/Sub destination and `unique_writer_identity` | `terraform/modules/feed_audit_notification_route/main.tf` | config | event-driven + pub-sub | Deployment repo has log-based metrics but no existing Logging sink resources. Use `02-RESEARCH.md` Pattern 1 and Terraform Google provider docs for the exact sink block. |
| Path-based Pub/Sub push endpoint to Cloud Run | `terraform/modules/feed_audit_notification_route/main.tf` | config | request-response | Existing Pub/Sub push subscriptions target bare service URIs. Combine their push pattern with the Cloud Scheduler `oidc_token.audience` pattern. |

## Out Of Scope For Pattern Copy

| File / Area | Reason |
|-------------|--------|
| `radio-transcription/backend/**` runtime files | Phase 2 is route infrastructure only. Phase 1 already owns the producer log contract. |
| `radio-transcription/terraform/modules/**` public Terraform modules | Research recommends a deployment-owned route because the route depends on private environment composition and relay Cloud Run resources. |
| `radio-transcription-deployment/.github/workflows/*.yml` | Current workflows already validate Terraform changes. Modify only if implementation adds required environment variables that CI/deploy must provide. |

## Metadata

**Analog search scope:** `/tmp/radio-transcription-deployment-main-14ac7c4/terraform`, `/tmp/radio-transcription-deployment-main-14ac7c4/.github/workflows`, `radio-transcription/.planning`

**Files scanned:** 98 deployment Terraform/workflow files plus phase artifacts

**Strong analogs used:** 5 (`message_queues`, `services/transcription`, `services/notification`, `modules/app`, environment roots)

**Pattern extraction date:** 2026-06-26
