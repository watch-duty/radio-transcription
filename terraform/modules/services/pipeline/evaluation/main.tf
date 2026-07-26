# =============================================================================
# EVALUATION SERVICE
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

# Cloud Run service that evaluates transcriptions against predefined rules
resource "google_cloud_run_v2_service" "evaluation_pipeline" {
  name     = "evaluation-pipeline-${var.environment}"
  location = var.region

  template {
    service_account = google_service_account.evaluation_pipeline_sa.email

    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = local.project_id
      }
      env {
        name  = "RULES_EVALUATION_RESULTS_TOPIC"
        value = var.topic_evaluated_audio_id
      }
      env {
        name  = "RULES_API_URL"
        value = var.rules_api_url
      }

      env {
        name  = "AUDIO_SEGMENTS_API_URL"
        value = var.audio_segments_api_url
      }
      env {
        name  = "IS_GCP"
        value = "true"
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

# Trigger the evaluation pipeline whenever a new transcription is published
# Push subscription from transcribed audio topic to the evaluation pipeline
resource "google_pubsub_subscription" "evaluation_pubsub_subscription" {
  name  = "evaluation-subscription-${var.environment}"
  topic = var.topic_transcribed_audio_id

  enable_message_ordering = true

  push_config {
    push_endpoint = google_cloud_run_v2_service.evaluation_pipeline.uri

    oidc_token {
      service_account_email = google_service_account.eventarc_invoker.email
    }
  }

  ack_deadline_seconds = 60

  dead_letter_policy {
    dead_letter_topic     = "projects/${local.project_id}/topics/${var.topic_evaluated_audio_dead_letter_name}"
    max_delivery_attempts = 5
  }

  depends_on = [
    google_project_iam_member.pubsub_token_creator,
    google_project_iam_member.eventarc_receiver,
    google_project_iam_member.eventarc_run_invoker,
    google_pubsub_topic_iam_member.evaluation_dlq_publisher
  ]
}

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

# Dedicated identity for the evaluation pipeline service
resource "google_service_account" "evaluation_pipeline_sa" {
  account_id   = "evaluation-pipeline-sa-${var.environment}"
  display_name = "Evaluation Pipeline Service Account"
}

# Allow evaluation pipeline to publish processed messages
# Note: Project-level roles are used here to resolve 403s when schema discovery is needed.
resource "google_project_iam_member" "evaluation_publisher" {
  project = local.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.evaluation_pipeline_sa.email}"
}

# Pub/Sub Viewer role helps with schema discovery/validation during publishing
resource "google_project_iam_member" "evaluation_pubsub_viewer" {
  project = local.project_id
  role    = "roles/pubsub.viewer"
  member  = "serviceAccount:${google_service_account.evaluation_pipeline_sa.email}"
}

# Allows the evaluation pipeline to invoke ONLY the rules management service
resource "google_cloud_run_v2_service_iam_member" "evaluation_rules_invoker" {
  project  = local.project_id
  location = var.region
  name     = var.rules_service_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.evaluation_pipeline_sa.email}"
}


resource "google_cloud_run_v2_service_iam_member" "evaluation_audio_segments_invoker" {
  project  = local.project_id
  location = var.region
  name     = var.audio_segments_service_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.evaluation_pipeline_sa.email}"
}

# Standard roles for logging and monitoring in Cloud Run
resource "google_project_iam_member" "evaluation_log_writer" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.evaluation_pipeline_sa.email}"
}

resource "google_project_iam_member" "evaluation_metric_writer" {
  project = local.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.evaluation_pipeline_sa.email}"
}

resource "google_project_iam_member" "evaluation_trace_writer" {
  project = local.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.evaluation_pipeline_sa.email}"
}

# -----------------------------------------------------------------------------
# Eventarc IAM
# TODO: Deprecate this in favor for PubSub subscriptions
# Currently holds a grab-bag of resources needed for Eventarc. Holding in here
# for now until we move to PubSub subscriptions instead so that it'll be easier
# to migrate.
# -----------------------------------------------------------------------------

# Service account for Eventarc triggers and push subscriptions
resource "google_service_account" "eventarc_invoker" {
  account_id = "eventarc-invoker-${var.environment}"
}

# Allow Eventarc/Subscribers to invoke protected Cloud Run services
resource "google_project_iam_member" "eventarc_run_invoker" {
  project = local.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.eventarc_invoker.email}"
}

# Grant required permissions for receiving events from Eventarc
resource "google_project_iam_member" "eventarc_receiver" {
  project = local.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.eventarc_invoker.email}"
}


# Allow evaluation Pub/Sub DLQ to be published to
resource "google_pubsub_topic_iam_member" "evaluation_dlq_publisher" {
  project = local.project_id
  topic   = var.topic_evaluated_audio_dead_letter_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Allow Pub/Sub to create identity tokens (required for push subscriptions and Eventarc)
resource "google_project_iam_member" "pubsub_token_creator" {
  project = local.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
