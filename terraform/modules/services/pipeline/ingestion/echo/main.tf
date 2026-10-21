# =============================================================================
# ECHO AUDIO INGESTION
# =============================================================================

locals {
  project_id     = var.project_id
  project_number = var.project_number
}

# -----------------------------------------------------------------------------
# ECHO RECORDINGS BUCKET
# -----------------------------------------------------------------------------

module "echo_recordings_bucket" {
  source = "../../../../gcs_bucket"

  project_id = local.project_id
  name       = var.echo_recordings_bucket_name
  location   = var.region

  labels = {
    environment = var.environment
    service     = "radio-transcription"
    source      = "echo"
    managed_by  = "terraform"
  }

  # In dev the recordings bucket is just a transit point for the prod-Echo
  # dual-write mirror — the canonical staged audio lives in the staging
  # bucket. Age objects out after 7 days to cap storage cost regardless of
  # prod volume. Prod is unaffected (var.prod_project_id is "" outside dev).
  lifecycle_rules = var.prod_project_id != "" ? [
    {
      action    = { type = "Delete" }
      condition = { age = 7 }
    }
  ] : []
}

# Service account for device uploads via HMAC keys
resource "google_service_account" "echo_recordings_uploader" {
  account_id   = "echo-uploader-${var.environment}"
  display_name = "Echo Recordings Uploader"
  description  = "Service account for uploading Echo recordings to GCS via HMAC keys"
}

resource "google_storage_bucket_iam_member" "echo_recordings_uploader" {
  bucket = module.echo_recordings_bucket.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.echo_recordings_uploader.email}"
}

resource "google_storage_hmac_key" "echo_recordings_uploader" {
  service_account_email = google_service_account.echo_recordings_uploader.email
}

# =============================================================================
# ECHO AUDIO INGESTION (Cloud Run v2)
# =============================================================================
#
# Triggered by Eventarc on GCS OBJECT_FINALIZE events from the Echo recordings
# bucket. Resolves feed metadata from AlloyDB, writes MP3 to the staging
# bucket, and publishes AudioChunk to the segmented-audio topic.
#
# Deployed as a Cloud Run service (not a Cloud Function) because the runtime
# requires ffmpeg (installed via Dockerfile) and protobuf compilation at build
# time — neither is available in the standard GCP Python buildpack.

resource "google_cloud_run_v2_service" "echo_ingestion" {
  name                = "echo-audio-ingestion-${var.environment}"
  location            = var.region
  deletion_protection = false
  ingress             = "INGRESS_TRAFFIC_INTERNAL_ONLY"

  depends_on = [
    google_secret_manager_secret_iam_member.echo_ingestion_secret,
  ]

  template {
    service_account = google_service_account.echo_ingestion.email

    max_instance_request_concurrency = 1
    timeout                          = "120s"

    scaling {
      min_instance_count = 1
      max_instance_count = var.echo_ingestion_max_instances
    }

    vpc_access {
      network_interfaces {
        network    = var.network_name
        subnetwork = var.subnet_name
      }
      egress = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      resources {
        limits = {
          cpu    = "1000m"
          memory = "1Gi"
        }
      }

      env {
        name  = "ALLOYDB_HOST"
        value = var.alloydb_primary_instance_ip
      }
      env {
        name  = "ALLOYDB_PORT"
        value = tostring(var.alloydb_connection_pooling_port)
      }
      env {
        name  = "ALLOYDB_USER"
        value = "worker"
      }
      env {
        name  = "ALLOYDB_DB"
        value = var.alloydb_database_name
      }
      env {
        name  = "AUDIO_STAGING_BUCKET"
        value = var.ingestion_staging_bucket_name
      }
      env {
        name  = "SEGMENTED_PUBSUB_TOPIC_PATH"
        value = var.topic_segmented_audio_id
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
        name  = "IS_INGESTION_SERVICE"
        value = "true"
      }
      env {
        name  = "FEED_AUDIT_ACTOR_ID"
        value = "service_account:gcp:${google_service_account.echo_ingestion.email}"
      }
      # Optional dev-mirror dual-write. Present only when
      # var.dev_recordings_bucket_name is set (prod only) — see variables.tf.
      dynamic "env" {
        for_each = var.dev_recordings_bucket_name != "" ? [1] : []
        content {
          name  = "DEV_RECORDINGS_BUCKET"
          value = var.dev_recordings_bucket_name
        }
      }
      env {
        name = "ALLOYDB_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = var.worker_password_secret_id
            version = "latest"
          }
        }
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

resource "google_eventarc_trigger" "echo_ingestion" {
  name     = "echo-audio-ingestion-${var.environment}"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }

  matching_criteria {
    attribute = "bucket"
    value     = var.echo_recordings_bucket_name
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.echo_ingestion.name
      region  = var.region
    }
  }

  service_account = google_service_account.echo_ingestion_trigger.email

  depends_on = [
    google_project_iam_member.echo_trigger_event_receiver,
    google_cloud_run_v2_service_iam_member.echo_trigger_invoker,
    google_project_iam_member.gcs_service_agent_pubsub_publisher,
  ]
}

resource "google_service_account" "echo_ingestion" {
  account_id   = "echo-audio-ingestion-${var.environment}"
  display_name = "Echo Audio Ingestion"
  description  = "Service account for the Echo audio ingestion Cloud Run service"
}

resource "google_service_account" "echo_ingestion_trigger" {
  account_id   = "echo-ingestion-trigger-${var.environment}"
  display_name = "Echo Ingestion Eventarc Trigger"
  description  = "Dedicated service account for the Eventarc trigger — only needs invoker and eventReceiver permissions"
}

# Read echo recordings bucket (download raw MP3)
resource "google_storage_bucket_iam_member" "echo_ingestion_reader" {
  bucket = module.echo_recordings_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.echo_ingestion.email}"
}

# Cross-project: allow the PROD Echo CF to write into THIS environment's
# recordings bucket as part of the dev-mirror dual-write. Only applied when
# var.prod_project_id is set (dev only). The prod CF runtime SA email is
# stable across dev rebuilds, so the binding is durable.
resource "google_storage_bucket_iam_member" "prod_echo_dev_mirror_writer" {
  count  = var.prod_project_id != "" ? 1 : 0
  bucket = module.echo_recordings_bucket.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:echo-audio-ingestion-prod@${var.prod_project_id}.iam.gserviceaccount.com"
}

# Write canonical bucket — objectCreator is sufficient because the handler
# never overwrites; if_generation_match=0 causes a server-side 412 rejection
# when the object already exists, which the code catches gracefully.
resource "google_storage_bucket_iam_member" "echo_ingestion_writer" {
  bucket = var.ingestion_staging_bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.echo_ingestion.email}"
}

# Publish to segmented-audio topic (with message ordering)
resource "google_pubsub_topic_iam_member" "echo_ingestion_publisher" {
  topic  = var.topic_segmented_audio_name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.echo_ingestion.email}"
}

# Project-level roles for the Echo Ingestion service account (AlloyDB client, tracing, logging, metrics)
resource "google_project_iam_member" "echo_ingestion_roles" {
  for_each = toset([
    "roles/alloydb.client",
    "roles/cloudtrace.agent",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
  ])

  project = local.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.echo_ingestion.email}"
}

# Eventarc trigger needs invoker permission on the Cloud Run service
resource "google_cloud_run_v2_service_iam_member" "echo_trigger_invoker" {
  name     = google_cloud_run_v2_service.echo_ingestion.name
  location = var.region
  project  = local.project_id
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.echo_ingestion_trigger.email}"
}

# Eventarc trigger needs eventReceiver to receive GCS events
resource "google_project_iam_member" "echo_trigger_event_receiver" {
  project = local.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.echo_ingestion_trigger.email}"
}

# GCS service agent needs pubsub.publisher for Eventarc GCS triggers.
# Without this, Eventarc silently fails to deliver OBJECT_FINALIZE events.
resource "google_project_iam_member" "gcs_service_agent_pubsub_publisher" {
  project = local.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${local.project_number}@gs-project-accounts.iam.gserviceaccount.com"
}

# Secret Manager access for DB password
resource "google_secret_manager_secret_iam_member" "echo_ingestion_secret" {
  project   = local.project_id
  secret_id = var.worker_password_secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.echo_ingestion.email}"
}
