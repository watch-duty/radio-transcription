data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
}

# Service account for the rules api service
resource "google_service_account" "rules_api_sa" {
  account_id   = "rules-management-${var.environment}"
  display_name = "Rules api Service Account"
  description  = "Service account for the Rules Management Cloud Run service"
}

# Allow rules api service to invoke protected Cloud Run services
resource "google_project_iam_member" "rules_api_run_invoker" {
  project = local.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.rules_api_sa.email}"
}

# Allow rules api service to access the AlloyDB worker password secret
resource "google_secret_manager_secret_iam_member" "rules_api_alloydb_secret_accessor" {
  project   = local.project_id
  secret_id = var.worker_password_secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.rules_api_sa.email}"
}

# Standard roles for logging, monitoring, and tracing in Cloud Run
resource "google_project_iam_member" "rules_api_cloudtrace_agent" {
  project = local.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.rules_api_sa.email}"
}

resource "google_project_iam_member" "rules_api_log_writer" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.rules_api_sa.email}"
}

resource "google_project_iam_member" "rules_api_metric_writer" {
  project = local.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.rules_api_sa.email}"
}

# Cloud Run service that manages transcription rules
resource "google_cloud_run_v2_service" "rules_api" {
  name                = "rules-management-${var.environment}"
  location            = var.region
  deletion_protection = false

  depends_on = [
    google_secret_manager_secret_iam_member.rules_api_alloydb_secret_accessor
  ]

  template {
    service_account = google_service_account.rules_api_sa.email

    vpc_access {
      network_interfaces {
        network    = var.network_name
        subnetwork = var.subnet_name
      }
      egress = "PRIVATE_RANGES_ONLY"
    }

    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder; managed by CI/CD

      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = local.project_id
      }

      env {
        name  = "ALLOYDB_HOST"
        value = var.alloydb_instance_ip
      }

      env {
        name  = "ALLOYDB_PORT"
        value = var.alloydb_connection_pooling_port
      }

      env {
        name  = "ALLOYDB_USER"
        value = "worker"
      }

      env {
        name  = "ALLOYDB_DB"
        value = var.radio_transcription_db_name
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
      env {
        name  = "IS_GCP"
        value = "true"
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
