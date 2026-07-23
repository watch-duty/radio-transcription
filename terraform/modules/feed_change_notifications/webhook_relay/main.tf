data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
}

# =============================================================================
# FEED CHANGE WEBHOOK RELAY
# =============================================================================

resource "google_secret_manager_secret" "webhook_api_key" {
  project   = local.project_id
  secret_id = "feed-change-webhook-api-key-${var.environment}"

  replication {
    auto {}
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_secret_manager_secret_version" "webhook_api_key" {
  secret      = google_secret_manager_secret.webhook_api_key.id
  secret_data = var.webhook_api_key
}

resource "google_service_account" "feed_change_webhook" {
  account_id   = "feed-change-webhook-${var.environment}"
  display_name = "Feed Change Webhook Relay Service Account"
  description  = "Runs the Feed Change Notifications relay Cloud Run service."
}

# Allow the Terraform deployer to attach the relay runtime service account to
# the Cloud Run service.
resource "google_service_account_iam_member" "deployer_feed_change_webhook_user" {
  count = var.deployer_service_account_email != null && var.deployer_service_account_email != "" ? 1 : 0

  service_account_id = google_service_account.feed_change_webhook.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.deployer_service_account_email}"
}

resource "google_cloud_run_v2_service" "feed_change_webhook" {
  name     = "feed-change-webhook-${var.environment}"
  location = var.region

  depends_on = [
    google_secret_manager_secret_iam_member.webhook_api_key_secret_access,
    google_service_account_iam_member.deployer_feed_change_webhook_user,
  ]

  template {
    service_account = google_service_account.feed_change_webhook.email
    # The relay uses native async HTTP delivery. Keep per-instance concurrency
    # aligned with the async HTTP client connection limit in the public repo.
    max_instance_request_concurrency = 80

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      resources {
        startup_cpu_boost = true
      }

      env {
        name  = "FEED_CHANGE_WEBHOOK_URL"
        value = var.webhook_url
      }
      env {
        name = "FEED_CHANGE_WEBHOOK_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.webhook_api_key.secret_id
            version = google_secret_manager_secret_version.webhook_api_key.version
          }
        }
      }
      env {
        name  = "IS_GCP"
        value = "true"
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = local.project_id
      }
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
      client,
      client_version,
    ]
  }
}

resource "google_secret_manager_secret_iam_member" "webhook_api_key_secret_access" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.webhook_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.feed_change_webhook.email}"
}

resource "google_project_iam_member" "feed_change_webhook_runtime_roles" {
  for_each = toset([
    "roles/cloudtrace.agent",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])

  project = local.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.feed_change_webhook.email}"
}
