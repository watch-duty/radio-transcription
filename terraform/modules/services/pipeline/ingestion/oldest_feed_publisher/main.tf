# =============================================================================
# OLDEST-FEED PUBLISHER (Cloud Run v2 + Cloud Scheduler)
# =============================================================================
# Per Phase 2 REQUIREMENTS.md PUB-01..PUB-10, IAM-01..IAM-07.
# Mirrors broadcastify_credential_rotation/ shape (Functions Framework on
# Cloud Run v2, Cloud Scheduler + OIDC). See ../broadcastify_credential_rotation/main.tf
# for the canonical pattern. Differences here are documented inline.

# ---------------------------------------------------------------------------
# SERVICE ACCOUNTS
# ---------------------------------------------------------------------------

# Publisher Cloud Run service account (IAM-01).
resource "google_service_account" "oldest_feed_publisher" {
  account_id   = "oldest-feed-publisher-${var.environment}"
  display_name = "Oldest Feed Publisher Service Account"
  description  = "Service account for the oldest-feed-publisher Cloud Run service (Phase 2 PUB-01)"
}

# Cloud Scheduler service account (IAM-06). Distinct from Publisher SA so
# the invoker role can be granted at the Cloud-Run-resource level (least
# privilege; Publisher SA itself does NOT have run.invoker on its own
# service).
resource "google_service_account" "oldest_feed_publisher_scheduler" {
  account_id   = "oldest-feed-pub-scheduler-${var.environment}"
  display_name = "Oldest Feed Publisher Scheduler"
  description  = "Service account Cloud Scheduler uses to mint OIDC tokens that invoke the Publisher (Phase 2 IAM-06)"
}

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

# Project-level roles for the Publisher service account (AlloyDB client, logging, metrics, tracing)
resource "google_project_iam_member" "publisher_roles" {
  for_each = toset([
    "roles/alloydb.client",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/cloudtrace.agent",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.oldest_feed_publisher.email}"
}

# IAM-03: Publisher SA gets roles/secretmanager.secretAccessor — SCOPED to
# the worker password secret only (not project-wide). Secret already exists
# (created by module.storage); we reference it by ID, do not create a new one.
resource "google_secret_manager_secret_iam_member" "publisher_password_accessor" {
  project   = var.project_id
  secret_id = var.worker_password_secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.oldest_feed_publisher.email}"
}

# IAM-07: Scheduler SA gets roles/run.invoker on the Publisher Cloud Run
# service. Service-resource-scoped (NOT project-wide) — least privilege.
resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.oldest_feed_publisher.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.oldest_feed_publisher_scheduler.email}"
}

# The Cloud Scheduler service agent must be allowed to mint OIDC tokens
# for the Scheduler SA in order for http_target.oidc_token to work. This
# binding is non-obvious — see broadcastify_credential_rotation/main.tf
# lines 121-126 for the canonical reference. Without it, Scheduler
# invocations fail with "Permission denied" minting the OIDC token.
resource "google_service_account_iam_member" "cloud_scheduler_token_creator" {
  service_account_id = google_service_account.oldest_feed_publisher_scheduler.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${var.project_number}@gcp-sa-cloudscheduler.iam.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# CLOUD RUN SERVICE (PUB-03, PUB-04, PUB-05, PUB-06)
# ---------------------------------------------------------------------------
#
# Mirrors broadcastify_credential_rotation (../broadcastify_credential_rotation/
# main.tf) for the Cloud Scheduler + OIDC invocation pattern, and echo_ingestion
# (../main.tf:343) for Direct VPC Egress + secret_key_ref + lifecycle
# ignore_changes. Differs from those:
# - timeout=30s (echo: 120s, bcfy_rotation: 540s) — Publisher's query is a
#   single AlloyDB COUNT(*) aggregate over indexed status='unclaimed' rows;
#   millisecond-class with PgBouncer connect overhead in tens of ms.
# - max_instance_count=1 + min_instance_count=0 (echo: 1/1) — Publisher scales
#   to zero between 60-second Scheduler ticks.
# - NO `ingress` field — defaults to INGRESS_TRAFFIC_ALL, matching
#   broadcastify_credential_rotation. Cloud Scheduler invokes via OIDC over
#   the public .run.app URL; INGRESS_TRAFFIC_INTERNAL_ONLY would block that
#   (Scheduler traffic doesn't originate inside the VPC). Security is
#   preserved entirely via the `roles/run.invoker` IAM binding restricted to
#   the Scheduler SA below — only that SA can mint a valid OIDC token for
#   this audience, so unauthenticated traffic is rejected at the IAM layer.
#
# Image: placeholder (us-docker.pkg.dev/cloudrun/container/hello). The real
# image at ${region}-docker.pkg.dev/${project}/radio-transcription-services-${env}/oldest-feed-publisher:latest
# is built + pushed by radio-transcription deploy CI from Plan 02-01's
# commit (see PHASE-2-PUBLISHER-SOURCE-REF.txt). lifecycle.ignore_changes
# prevents Terraform from clobbering the CI-pushed image on subsequent
# applies — same pattern as echo_ingestion + broadcastify_credential_rotation.
resource "google_cloud_run_v2_service" "oldest_feed_publisher" {
  name                = "oldest-feed-publisher-${var.environment}"
  location            = var.region
  deletion_protection = false

  depends_on = [
    google_secret_manager_secret_iam_member.publisher_password_accessor,
  ]

  template {
    service_account = google_service_account.oldest_feed_publisher.email

    timeout                          = "30s"
    max_instance_request_concurrency = 1

    scaling {
      min_instance_count = 0
      max_instance_count = 1
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
          memory = "512Mi"
        }
      }

      env {
        name  = "GOOGLE_CLOUD_PROJECT"
        value = var.project_id
      }
      env {
        name  = "IS_GCP"
        value = "true"
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

# ---------------------------------------------------------------------------
# CLOUD SCHEDULER (PUB-10)
# ---------------------------------------------------------------------------
# Per CONTEXT D-01 + Claude's Discretion: retry_count=0 (the next minute's
# tick handles transient failures naturally; retries within the 30s deadline
# add cost without benefit at 60s cadence).
resource "google_cloud_scheduler_job" "oldest_feed_publisher" {
  name             = "oldest-feed-publisher-${var.environment}"
  project          = var.project_id
  region           = var.region
  schedule         = "* * * * *" # every minute
  time_zone        = "UTC"
  attempt_deadline = "30s"
  paused           = false

  retry_config {
    retry_count = 0
  }

  http_target {
    uri         = google_cloud_run_v2_service.oldest_feed_publisher.uri
    http_method = "POST"

    oidc_token {
      service_account_email = google_service_account.oldest_feed_publisher_scheduler.email
      audience              = google_cloud_run_v2_service.oldest_feed_publisher.uri
    }
  }

  depends_on = [
    google_cloud_run_v2_service_iam_member.scheduler_invoker,
    google_service_account_iam_member.cloud_scheduler_token_creator,
  ]
}
