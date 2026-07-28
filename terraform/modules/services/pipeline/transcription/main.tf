# =============================================================================
# TRANSCRIPTION SERVICE
# =============================================================================

data "google_project" "project" {}

locals {
  project_id              = data.google_project.project.project_id
  project_number          = data.google_project.project.number
  otel_traces_sampler     = var.environment == "dev" ? "parentbased_traceidratio" : "parentbased_always_on"
  otel_traces_sampler_arg = var.environment == "dev" ? "0.05" : "1.0"
  otel_bsp_max_batch_size = var.environment == "dev" ? "512" : "64"
  otel_bsp_schedule_delay = var.environment == "dev" ? "5000" : "1000"
}

# Cloud Run service that transcribes normalized audio ready for transcription
resource "google_cloud_run_v2_service" "transcription_service" {
  name     = "transcription-service-${var.environment}"
  location = var.region

  template {
    service_account                  = google_service_account.transcription_service_sa.email
    max_instance_request_concurrency = var.concurrency

    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      resources {
        startup_cpu_boost = true
      }

      env {
        name  = "PROJECT_ID"
        value = local.project_id
      }
      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = local.project_id
      }
      env {
        name  = "IS_GCP"
        value = "true"
      }
      env {
        name  = "OUTPUT_TOPIC"
        value = var.topic_transcribed_audio_id
      }
      env {
        name  = "TRANSCRIBER_TYPE"
        value = var.transcriber_type
      }
      env {
        name  = "TRANSCRIBER_CONFIG"
        value = var.transcriber_config
      }
      env {
        name  = "AUDIO_SEGMENTS_API_URL"
        value = var.audio_segments_api_url
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
  }

  lifecycle {
    ignore_changes = [template[0].containers[0].image]
  }
}

# Trigger the transcription service whenever new normalized audio is ready
# Push subscription from audio-ready topic to the transcription service
resource "google_pubsub_subscription" "transcription_pubsub_subscription" {
  name  = "transcription-subscription-${var.environment}"
  topic = var.topic_normalized_audio_id

  enable_message_ordering = true

  push_config {
    push_endpoint = google_cloud_run_v2_service.transcription_service.uri

    oidc_token {
      service_account_email = google_service_account.pubsub_invoker.email
    }
  }

  ack_deadline_seconds = 120 # 2 minutes (aligned with client-side transcription timeout)

  dead_letter_policy {
    dead_letter_topic     = "projects/${local.project_id}/topics/${var.topic_transcribed_audio_dlq_name}"
    max_delivery_attempts = 5
  }

  depends_on = [
    google_project_iam_member.pubsub_token_creator,
    google_cloud_run_v2_service_iam_member.pubsub_invoker_run_invoker
  ]
}

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

# Dedicated identity for the transcription service
resource "google_service_account" "transcription_service_sa" {
  account_id   = "transcription-sa-${var.environment}"
  display_name = "Transcription Service Account"
}

# Grant necessary permissions to the transcription service account
resource "google_project_iam_member" "transcription_publisher" {
  project = local.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

resource "google_project_iam_member" "transcription_pubsub_viewer" {
  project = local.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

resource "google_project_iam_member" "transcription_speech_client" {
  project = local.project_id
  role    = "roles/speech.client"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

resource "google_project_iam_member" "transcription_vertex_user" {
  project = local.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

resource "google_project_iam_member" "transcription_storage_viewer" {
  project = local.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

# Standard roles for logging and monitoring in Cloud Run
resource "google_project_iam_member" "transcription_log_writer" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

resource "google_project_iam_member" "transcription_metric_writer" {
  project = local.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

resource "google_project_iam_member" "transcription_trace_writer" {
  project = local.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

# Allow transcription service to invoke the audio segments API
resource "google_cloud_run_v2_service_iam_member" "transcription_audio_segments_invoker" {
  project  = local.project_id
  location = var.region
  name     = var.audio_segments_service_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.transcription_service_sa.email}"
}

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
  project  = local.project_id
  location = var.region
  name     = google_cloud_run_v2_service.transcription_service.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}


# Allow Pub/Sub to publish failed messages to the Dead Letter Queue
resource "google_pubsub_topic_iam_member" "transcription_dlq_publisher" {
  project = local.project_id
  topic   = var.topic_transcribed_audio_dlq_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allows Pub/Sub SA to pull and acknowledge messages that will be put on the DLQ
resource "google_pubsub_subscription_iam_member" "transcription_pubsub_subscriber" {
  project      = local.project_id
  subscription = google_pubsub_subscription.transcription_pubsub_subscription.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allow Pub/Sub service to create identity tokens for push endpoints
resource "google_project_iam_member" "pubsub_token_creator" {
  project = local.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
