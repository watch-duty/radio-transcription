# =============================================================================
# NOTIFICATION PIPELINE
# =============================================================================

data "google_project" "project" {}

locals {
  project_id              = data.google_project.project.project_id
  project_number          = data.google_project.project.number
  redis_certificate_path  = "/etc/secrets"
  otel_traces_sampler     = var.environment == "dev" ? "parentbased_traceidratio" : "parentbased_always_on"
  otel_traces_sampler_arg = var.environment == "dev" ? "0.05" : "1.0"
  otel_bsp_max_batch_size = var.environment == "dev" ? "512" : "64"
  otel_bsp_schedule_delay = var.environment == "dev" ? "5000" : "1000"
}

# Secret Manager resource for the external backend API key
resource "google_secret_manager_secret" "external_endpoint_api_key" {
  project   = local.project_id
  secret_id = "wd-backend-endpoint-API-key"
  replication {
    auto {}
  }
}

# Store the user-provided API key as a secret version
resource "google_secret_manager_secret_version" "external_endpoint_api_key" {
  secret      = google_secret_manager_secret.external_endpoint_api_key.id
  secret_data = var.external_endpoint_api_key
}

# Cloud Run service that pushes alerts to external backend endpoints
resource "google_cloud_run_v2_service" "notification_pipeline" {
  name     = "notification-pipeline-${var.environment}"
  location = var.region

  depends_on = [
    google_secret_manager_secret_iam_member.external_endpoint_api_key_secret_access,
    google_secret_manager_secret_iam_member.notification_pipeline_redis_password_secret_access,
    google_secret_manager_secret_iam_member.notification_pipeline_redis_certificate_secret_access
  ]

  template {
    service_account = google_service_account.notification_pipeline_sa.email

    vpc_access {
      network_interfaces {
        network    = var.network_name
        subnetwork = var.subnet_name
      }

      egress = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD
      volume_mounts {
        name       = "notification-pipeline-redis-certificate"
        mount_path = local.redis_certificate_path
      }

      env {
        name  = "NOTIFICATION_ENDPOINT"
        value = var.external_endpoint
      }
      env {
        name = "NOTIFICATION_ENDPOINT_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.external_endpoint_api_key.secret_id
            version = "latest"
          }
        }
      }
      env {
        name  = "REDIS_HOST"
        value = var.redis_host
      }
      env {
        name  = "REDIS_PORT"
        value = var.redis_port
      }
      env {
        name = "REDIS_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = var.redis_password_secret_id
            version = "latest"
          }
        }
      }
      env {
        name  = "REDIS_CERTIFICATE_PATH"
        value = "${local.redis_certificate_path}/server_ca.pem"
      }
      env {
        name  = "IS_GCP"
        value = "true"
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = local.project_id
      }
      env {
        name  = "APP_URL"
        value = var.webapp_base_url
      }
      env {
        name  = "FEEDS_API_URL"
        value = var.feeds_api_url
      }
      env {
        name  = "RULES_API_URL"
        value = var.rules_api_url
      }
      env {
        name  = "OTEL_TRACES_SAMPLER"
        value = local.otel_traces_sampler
      }
      env {
        name  = "OTEL_TRACES_SAMPLER_ARG"
        value = local.otel_traces_sampler_arg
      }
      env {
        name  = "OTEL_BSP_MAX_EXPORT_BATCH_SIZE"
        value = local.otel_bsp_max_batch_size
      }
      env {
        name  = "OTEL_BSP_SCHEDULE_DELAY"
        value = local.otel_bsp_schedule_delay
      }
    }

    volumes {
      name = "notification-pipeline-redis-certificate"
      secret {
        secret = var.redis_certificate_secret_id
        items {
          version = "latest"
          path    = "server_ca.pem"
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].containers[0].image]
  }
}

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

# =============================================================================
# IAM
# =============================================================================
# Service account for the notification pipeline
resource "google_service_account" "notification_pipeline_sa" {
  account_id   = "notification-pipeline-${var.environment}"
  display_name = "Notification Pipeline Service Account"
  description  = "Service account for the notification pipeline to run"
}

# Allow notification pipeline to access the backend API key from Secret Manager
resource "google_secret_manager_secret_iam_member" "external_endpoint_api_key_secret_access" {
  project   = local.project_id
  secret_id = google_secret_manager_secret.external_endpoint_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.notification_pipeline_sa.email}"
}

# Allow notification pipeline to access the Memorystore AUTH string password from Secret Manager
resource "google_secret_manager_secret_iam_member" "notification_pipeline_redis_password_secret_access" {
  project   = local.project_id
  secret_id = var.redis_password_secret_full_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.notification_pipeline_sa.email}"
}

# Allow notification pipeline to access the Redis cert from Secret Manager
resource "google_secret_manager_secret_iam_member" "notification_pipeline_redis_certificate_secret_access" {
  project   = local.project_id
  secret_id = var.redis_certificate_secret_full_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.notification_pipeline_sa.email}"
}

# Project-level roles for the Notification service account (run invoker, tracing, logging, metrics)
resource "google_project_iam_member" "notification_pipeline_roles" {
  for_each = toset([
    "roles/run.invoker",
    "roles/cloudtrace.agent",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])

  project = local.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.notification_pipeline_sa.email}"
}

# Allows Pub/Sub SA to pull and acknowledge messages that will be put on the DLQ
resource "google_pubsub_subscription_iam_member" "alert_notification_pubsub_subscriber" {
  subscription = google_pubsub_subscription.alert_notification_pubsub_subscription.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
