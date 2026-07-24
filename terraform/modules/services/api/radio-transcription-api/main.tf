data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id

  openapi_content = templatefile("${path.module}/../../../../../frontend/api/openapi.yaml", {
    radio_transcription_api_url          = google_cloud_run_v2_service.radio_transcription_api.uri
    radio_transcription_api_service_name = google_api_gateway_api.radio_transcription_api_gw.managed_service
    google_auth_client_id                = var.google_auth_client_id
  })
}

# =============================================================================
# RADIO TRANSCRIPTION API MODULE
# =============================================================================

# Cloud Run service for the radio transcription API
resource "google_cloud_run_v2_service" "radio_transcription_api" {
  name     = "radio-transcription-api-${var.environment}"
  location = var.region

  template {
    service_account = google_service_account.radio_transcription_api_sa.email

    scaling {
      min_instance_count = 1
      max_instance_count = 100
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      env {
        name  = "GOOGLE_AUTH_CLIENT_ID"
        value = var.google_auth_client_id
      }
      env {
        name  = "GOOGLE_AUTH_CLIENT_SECRET"
        value = var.google_auth_client_secret
      }
      # The website origin to allow CORS request from
      env {
        name  = "ALLOWED_ORIGIN"
        value = var.allowed_origin
      }
      env {
        name  = "RULES_API_URL"
        value = var.rules_api_url
      }
      env {
        name  = "FEEDS_STORE_API_URL"
        value = var.feeds_api_url
      }
      env {
        name  = "AUDIO_SEGMENTS_API_URL"
        value = var.audio_segments_api_url
      }
      env {
        name  = "PROJECT_ID"
        value = local.project_id
      }
      env {
        # Used by Swagger spec
        name  = "API_PUBLIC_URL"
        value = var.api_public_url
      }
      env {
        name  = "IS_GCP"
        value = "true"
      }
      env {
        name  = "WORKSPACE_ADMIN_GROUP_EMAIL"
        value = var.workspace_admin_group_email
      }
    }
  }

  lifecycle {
    ignore_changes = [template[0].containers[0].image]
  }
}

# API Gateway for the radio transcription API
resource "google_api_gateway_api" "radio_transcription_api_gw" {
  provider = google-beta
  project  = local.project_id
  api_id   = "radio-transcription-api-${var.environment}"
}

resource "google_api_gateway_api_config" "radio_transcription_api_gw_config" {
  provider      = google-beta
  project       = local.project_id
  api           = google_api_gateway_api.radio_transcription_api_gw.api_id
  api_config_id = "cfg-${var.environment}-${substr(sha256(local.openapi_content), 0, 8)}"

  openapi_documents {
    document {
      path     = "spec.yaml"
      contents = base64encode(local.openapi_content)
    }
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    google_api_gateway_api.radio_transcription_api_gw
  ]
}

resource "google_api_gateway_gateway" "radio_transcription_api_gw_gateway" {
  provider   = google-beta
  project    = local.project_id
  api_config = google_api_gateway_api_config.radio_transcription_api_gw_config.id
  gateway_id = "radio-transcription-api-gateway-${var.environment}"

  depends_on = [
    google_api_gateway_api_config.radio_transcription_api_gw_config
  ]
}

# =============================================================================
# IAM
# =============================================================================

# Service account for the radio transcription API
resource "google_service_account" "radio_transcription_api_sa" {
  account_id   = "radio-transcription-api-${var.environment}"
  display_name = "Radio Transcription API Service Account"
  description  = "Service account for the radio transcription API Cloud Run service"
}

# Allow radio transcription API to invoke protected Cloud Run services
resource "google_project_iam_member" "radio_transcription_api_run_invoker" {
  project = local.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.radio_transcription_api_sa.email}"
}

resource "google_project_iam_member" "radio_transcription_api_apigateway_viewer" {
  project = local.project_id
  role    = "roles/apigateway.viewer"
  member  = "serviceAccount:${google_service_account.radio_transcription_api_sa.email}"
}

# Standard roles for logging, monitoring, and tracing in Cloud Run
resource "google_project_iam_member" "radio_transcription_api_cloudtrace_agent" {
  project = local.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.radio_transcription_api_sa.email}"
}

resource "google_project_iam_member" "radio_transcription_api_log_writer" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.radio_transcription_api_sa.email}"
}

resource "google_project_iam_member" "radio_transcription_api_metric_writer" {
  project = local.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.radio_transcription_api_sa.email}"
}
