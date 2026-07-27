# =============================================================================
# NORMALIZATION CLOUD RUN SERVICE
# =============================================================================

data "google_project" "project" {}

locals {
  project_id              = data.google_project.project.project_id
  project_number          = data.google_project.project.number
  concurrency             = 4
  uploads_per_request     = 3
  otel_traces_sampler     = var.environment == "dev" ? "parentbased_traceidratio" : "parentbased_always_on"
  otel_traces_sampler_arg = var.environment == "dev" ? "0.05" : "1.0"
  otel_bsp_max_batch_size = var.environment == "dev" ? "512" : "64"
  otel_bsp_schedule_delay = var.environment == "dev" ? "5000" : "1000"
}

# Cloud Run service that normalizes stitched audio ready for transcription
resource "google_cloud_run_v2_service" "normalization_service" {
  name     = "normalization-service-${var.environment}"
  location = var.region

  template {
    service_account = google_service_account.normalization_service_sa.email
    # Sizing model:
    # - Normalization is CPU-bound: each request runs ffmpeg/transcoding + waveform work.
    # - Keep CPU:memory:concurrency roughly balanced so one instance is not oversubscribed.
    # - Current target is 4 concurrent requests per instance with 2 vCPU / 2Gi memory.
    # - Max instances controls burst capacity and cost ceiling:
    #     max_inflight_requests = max_instance_count * max_instance_request_concurrency
    #   For max_instance_count = 50 and concurrency = 4, max in-flight capacity is 200.
    # - Revalidate by sampling real GCS raw_segments, running the normalization path
    #   concurrently, and checking worker latency + peak RSS against desired headroom.
    max_instance_request_concurrency = local.concurrency

    scaling {
      min_instance_count = 0
      # Limit max instances to 50 to maintain the same cost and resource ceiling
      # (100 total vCPUs / 100GiB) as the previous 1 vCPU / 100 max instances shape.
      max_instance_count = var.max_instance_count
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      resources {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
        startup_cpu_boost = true
      }

      env {
        name  = "PROJECT_ID"
        value = local.project_id
      }
      env {
        name  = "DLQ_TOPIC"
        value = var.topic_normalized_audio_dlq_name
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
        name  = "AUDIO_CANONICAL_BUCKET"
        value = var.canonical_audio_bucket_name
      }
      env {
        name  = "OUTPUT_TOPIC"
        value = var.topic_normalized_audio_id
      }
      env {
        name  = "AUDIO_SEGMENTS_API_URL"
        value = var.audio_segments_api_url
      }
      env {
        name  = "MAX_UPLOAD_THREADS"
        value = local.concurrency * local.uploads_per_request
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

# Trigger the normalization service whenever new segmented audio is ready
# Push subscription from segmented-audio-claims topic to the normalization service
resource "google_pubsub_subscription" "normalization_pubsub_subscription" {
  name  = "normalization-subscription-${var.environment}"
  topic = var.topic_segmented_audio_id

  enable_message_ordering = true

  push_config {
    push_endpoint = google_cloud_run_v2_service.normalization_service.uri

    oidc_token {
      service_account_email = google_service_account.pubsub_invoker.email
    }
  }

  ack_deadline_seconds = 600 # 10 minutes (normalization might take time for longer files)

  dead_letter_policy {
    dead_letter_topic     = "projects/${local.project_id}/topics/${var.topic_normalized_audio_dlq_name}"
    max_delivery_attempts = 5
  }

  depends_on = [
    google_project_iam_member.pubsub_token_creator,
    google_cloud_run_v2_service_iam_member.pubsub_invoker_run_invoker,
    google_pubsub_topic_iam_member.normalization_dlq_publisher
  ]
}



# =============================================================================
# IAM
# =============================================================================

# Dedicated identity for the normalization service
resource "google_service_account" "normalization_service_sa" {
  account_id   = "normalization-sa-${var.environment}"
  display_name = "Normalization Service Account"
}

# Grant necessary permissions to the normalization service account
resource "google_project_iam_member" "normalization_publisher" {
  project = local.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

resource "google_project_iam_member" "normalization_pubsub_viewer" {
  project = local.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

# Allow normalization service to read from staging bucket and write to canonical bucket
resource "google_storage_bucket_iam_member" "staging_viewer" {
  bucket = var.staging_audio_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

resource "google_storage_bucket_iam_member" "canonical_admin" {
  bucket = var.canonical_audio_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

# Standard roles for logging and monitoring in Cloud Run
resource "google_project_iam_member" "normalization_log_writer" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

resource "google_project_iam_member" "normalization_metric_writer" {
  project = local.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

resource "google_project_iam_member" "normalization_trace_writer" {
  project = local.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

# Allow normalization service to invoke the audio segments API
resource "google_cloud_run_v2_service_iam_member" "normalization_audio_segments_invoker" {
  project  = local.project_id
  location = var.region
  name     = var.audio_segments_service_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.normalization_service_sa.email}"
}

# -----------------------------------------------------------------------------
# Pub/Sub Push Invoker IAM
# -----------------------------------------------------------------------------

# Service account for the Pub/Sub push invoker
resource "google_service_account" "pubsub_invoker" {
  account_id   = "normalize-pubsub-invoker-${var.environment}"
  display_name = "Normalization Pub/Sub Push Invoker"
}

# Allow Pub/Sub invoker to call our Cloud Run service
resource "google_cloud_run_v2_service_iam_member" "pubsub_invoker_run_invoker" {
  project  = local.project_id
  location = var.region
  name     = google_cloud_run_v2_service.normalization_service.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}


# Allow Pub/Sub to publish failed messages to the Dead Letter Queue
resource "google_pubsub_topic_iam_member" "normalization_dlq_publisher" {
  project = local.project_id
  topic   = var.topic_normalized_audio_dlq_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allows Pub/Sub SA to pull and acknowledge messages that will be put on the DLQ
resource "google_pubsub_subscription_iam_member" "normalization_pubsub_subscriber" {
  project      = local.project_id
  subscription = google_pubsub_subscription.normalization_pubsub_subscription.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}



# Allow Pub/Sub service to create identity tokens for push endpoints
resource "google_project_iam_member" "pubsub_token_creator" {
  project = local.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
