data "google_project" "project" {}

locals {
  project_id     = data.google_project.project.project_id
  project_number = data.google_project.project.number
}

# =============================================================================
# BROADCASTIFY CREDENTIAL ROTATION
# =============================================================================


# Secret Manager resource for the Broadcastify API key
resource "google_secret_manager_secret" "broadcastify_api_key" {
  project   = local.project_id
  secret_id = "broadcastify-api-key-${var.environment}"
  replication {
    auto {}
  }
}

# Store the Broadcastify API key as a secret version
resource "google_secret_manager_secret_version" "broadcastify_api_key" {
  secret      = google_secret_manager_secret.broadcastify_api_key.id
  secret_data = var.broadcastify_api_key
}

# Secret Manager resource for the Broadcastify API key ID
resource "google_secret_manager_secret" "broadcastify_api_key_id" {
  project   = local.project_id
  secret_id = "broadcastify-api-key-id-${var.environment}"
  replication {
    auto {}
  }
}

# Store the Broadcastify API key ID as a secret version
resource "google_secret_manager_secret_version" "broadcastify_api_key_id" {
  secret      = google_secret_manager_secret.broadcastify_api_key_id.id
  secret_data = var.broadcastify_api_key_id
}

# Secret Manager resource for the Broadcastify API app ID
resource "google_secret_manager_secret" "broadcastify_api_app_id" {
  project   = local.project_id
  secret_id = "broadcastify-api-app-id-${var.environment}"
  replication {
    auto {}
  }
}

# Store the Broadcastify API app ID as a secret version
resource "google_secret_manager_secret_version" "broadcastify_api_app_id" {
  secret      = google_secret_manager_secret.broadcastify_api_app_id.id
  secret_data = var.broadcastify_api_app_id
}

# Secret Manager resource for the Broadcastify Auth JWT token (rotated)
resource "google_secret_manager_secret" "broadcastify_jwt" {
  project   = local.project_id
  secret_id = "broadcastify-jwt-${var.environment}"
  replication {
    auto {}
  }
}

# =============================================================================
# IAM
# =============================================================================

# Service account for the Broadcastify credential rotation function
resource "google_service_account" "broadcastify_credential_rotation_sa" {
  account_id   = "broadcastify-cred-rot-${var.environment}"
  display_name = "Broadcastify Credential Rotation Service Account"
  description  = "Service account for the Broadcastify credential rotation function"
}

# Allow the credential rotation function to read the Broadcastify API key
resource "google_secret_manager_secret_iam_member" "broadcastify_api_key_secret_access" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.broadcastify_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.broadcastify_credential_rotation_sa.email}"
}

# Allow the credential rotation function to read the Broadcastify API key ID
resource "google_secret_manager_secret_iam_member" "broadcastify_api_key_id_secret_access" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.broadcastify_api_key_id.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.broadcastify_credential_rotation_sa.email}"
}

# Allow the credential rotation function to read the Broadcastify API app ID
resource "google_secret_manager_secret_iam_member" "broadcastify_api_app_id_secret_access" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.broadcastify_api_app_id.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.broadcastify_credential_rotation_sa.email}"
}

# Allow the credential rotation function to read the Broadcastify Auth JWT token
resource "google_secret_manager_secret_iam_member" "broadcastify_jwt_secret_access" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.broadcastify_jwt.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.broadcastify_credential_rotation_sa.email}"
}

# Allow the credential rotation function to add, list, and delete versions of the Auth JWT token
resource "google_secret_manager_secret_iam_member" "broadcastify_jwt_secret_version_manager" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.broadcastify_jwt.id
  role      = "roles/secretmanager.secretVersionManager"
  member    = "serviceAccount:${google_service_account.broadcastify_credential_rotation_sa.email}"
}

# Allow the rotation function SA to invoke the Cloud Run service (used by Cloud Scheduler OIDC auth)
resource "google_cloud_run_v2_service_iam_member" "broadcastify_credential_rotation_run_invoker" {
  project  = local.project_id
  location = var.region
  name     = google_cloud_run_v2_service.broadcastify_credential_rotation.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.broadcastify_credential_rotation_sa.email}"
}

# Allow the Cloud Scheduler service agent to mint OIDC tokens for the rotation SA
resource "google_service_account_iam_member" "cloud_scheduler_token_creator" {
  service_account_id = google_service_account.broadcastify_credential_rotation_sa.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${local.project_number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
}

# Project-level roles for the Broadcastify credential rotation service account (logging, metrics, tracing)
resource "google_project_iam_member" "broadcastify_credential_rotation_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/cloudtrace.agent",
  ])

  project = local.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.broadcastify_credential_rotation_sa.email}"
}

# =============================================================================
# CLOUD SCHEDULER ROTATION TRIGGER
# =============================================================================

# Cloud Scheduler job that fires every 30 minutes to trigger credential rotation.
resource "google_cloud_scheduler_job" "broadcastify_credential_rotation" {
  name      = "broadcastify-credential-rotation-${var.environment}"
  project   = local.project_id
  region    = var.region
  schedule  = "*/30 * * * *"
  time_zone = "UTC"
  paused    = false

  retry_config {
    retry_count          = 3
    min_backoff_duration = "5s"
    max_backoff_duration = "30s"
  }

  http_target {
    uri         = google_cloud_run_v2_service.broadcastify_credential_rotation.uri
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.broadcastify_credential_rotation_sa.email
      audience              = google_cloud_run_v2_service.broadcastify_credential_rotation.uri
    }
  }

  depends_on = [
    google_cloud_run_v2_service_iam_member.broadcastify_credential_rotation_run_invoker,
    google_service_account_iam_member.cloud_scheduler_token_creator,
  ]
}

# =============================================================================
# MONITORING
# =============================================================================

resource "google_logging_metric" "broadcastify_cred_rot_failures" {
  name = "broadcastify-cred-rot-failures-${var.environment}"
  filter = join(" AND ", [
    "resource.type=\"cloud_scheduler_job\"",
    "resource.labels.job_id=\"${google_cloud_scheduler_job.broadcastify_credential_rotation.name}\"",
    "severity>=ERROR"
  ])

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

resource "google_logging_metric" "broadcastify_cred_rot_run_errors" {
  name = "broadcastify-cred-rot-run-errors-${var.environment}"
  filter = join(" AND ", [
    "resource.type=\"cloud_run_revision\"",
    "resource.labels.service_name=\"${google_cloud_run_v2_service.broadcastify_credential_rotation.name}\"",
    "httpRequest.status>=500"
  ])

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
  }
}

# GCP has an eventual-consistency window between creating a google_logging_metric
# and being able to reference it from a google_monitoring_alert_policy: during
# the 2026-04-20 prod apply, alert-policy creation failed with
#   "Error 404: Cannot find metric(s) that match type = ... If a metric was
#    created recently, it could take up to 10 minutes to become available."
# which left the apply partially complete and put terraform state into a
# deposed-orphan wedge that blocked every subsequent plan. The provider does
# not wait on its own; this sleep gives GCP time to propagate new metrics
# before the dependent alert policies are created.
#
# Apply this pattern for every future google_logging_metric ->
# google_monitoring_alert_policy edge in this codebase.
resource "time_sleep" "wait_for_log_metric_propagation" {
  depends_on = [
    google_logging_metric.broadcastify_cred_rot_failures,
    google_logging_metric.broadcastify_cred_rot_run_errors,
  ]
  create_duration = "120s"

  # Re-sleep when either metric is recreated (not just on first apply).
  triggers = {
    metric_ids = join(",", [
      google_logging_metric.broadcastify_cred_rot_failures.id,
      google_logging_metric.broadcastify_cred_rot_run_errors.id,
    ])
  }
}

resource "google_monitoring_alert_policy" "broadcastify_credential_rotation_failed_executions" {
  project      = local.project_id
  display_name = "Broadcastify credential rotation job failed (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  depends_on = [time_sleep.wait_for_log_metric_propagation]

  documentation {
    mime_type = "text/markdown"
    content   = "Alerts when the Cloud Scheduler rotation job reports a failed execution, which indicates retries were exhausted and the run did not succeed."
  }

  conditions {
    display_name = "Cloud Scheduler failed executions > 0"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.broadcastify_cred_rot_failures.name}\"",
        "resource.type=\"cloud_scheduler_job\""
      ])

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "broadcastify_credential_rotation_run_errors" {
  project      = local.project_id
  display_name = "Broadcastify credential rotation Cloud Run errors (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  depends_on = [time_sleep.wait_for_log_metric_propagation]

  documentation {
    mime_type = "text/markdown"
    content   = "Alerts when the broadcastify-credential-rotation Cloud Run service returns a 5xx HTTP error response. This may indicate upstream connectivity issues such as timeouts reaching the Broadcastify API (e.g. MaxRetryError / ReadTimeoutError against api.bcfy.io)."
  }

  conditions {
    display_name = "Cloud Run 5xx errors > 0"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.broadcastify_cred_rot_run_errors.name}\"",
        "resource.type=\"cloud_run_revision\""
      ])

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }
}

# =============================================================================
# Cloud Run Service
# =============================================================================

# Cloud Run service for the Broadcastify credential rotation function
resource "google_cloud_run_v2_service" "broadcastify_credential_rotation" {
  name     = "broadcastify-credential-rotation-${var.environment}"
  location = var.region

  depends_on = [
    google_secret_manager_secret_iam_member.broadcastify_api_key_secret_access,
    google_secret_manager_secret_iam_member.broadcastify_api_key_id_secret_access,
    google_secret_manager_secret_iam_member.broadcastify_api_app_id_secret_access,
    google_secret_manager_secret_iam_member.broadcastify_jwt_secret_access,
    google_secret_manager_secret_iam_member.broadcastify_jwt_secret_version_manager,
  ]

  template {
    service_account = google_service_account.broadcastify_credential_rotation_sa.email

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = local.project_id
      }

      env {
        name  = "IS_GCP"
        value = "true"
      }

      env {
        name = "BROADCASTIFY_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.broadcastify_api_key.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "BROADCASTIFY_API_KEY_ID"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.broadcastify_api_key_id.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "BROADCASTIFY_API_APP_ID"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.broadcastify_api_app_id.secret_id
            version = "latest"
          }
        }
      }

      env {
        name  = "BROADCASTIFY_USERNAME"
        value = var.broadcastify_username
      }

      env {
        name  = "BROADCASTIFY_PASSWORD"
        value = var.broadcastify_password
      }

      # Secret IDs for the rotation function to write new token versions via the Secret Manager API
      env {
        name  = "BROADCASTIFY_JWT_SECRET_ID"
        value = google_secret_manager_secret.broadcastify_jwt.secret_id
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].containers[0].image]
  }
}
