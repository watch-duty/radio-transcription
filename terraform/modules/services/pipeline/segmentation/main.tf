# =============================================================================
# SEGMENTATION SERVICE (DATAFLOW WORKERS)
# =============================================================================

data "google_project" "project" {}

locals {
  project_id     = data.google_project.project.project_id
  project_number = data.google_project.project.number
}

# Service account for the segmentation pipeline (Dataflow workers)
resource "google_service_account" "segmentation_pipeline_sa" {
  account_id   = "segmentation-pipeline-${var.environment}"
  display_name = "Segmentation Pipeline Service Account"
  description  = "Service account for the segmentation pipeline to run"
}

# Grant Category Admin roles at the project level to the worker service account.
# This ensures Dataflow can manage its own temporary Pub/Sub subscriptions
# and access all necessary GCS buckets without granular whitelisting.
resource "google_project_iam_member" "segmentation_worker_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/dataflow.viewer",
    "roles/pubsub.admin",
    "roles/storage.admin",
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/artifactregistry.reader",
    "roles/cloudtrace.agent",
    "roles/datalineage.editor"
  ])

  project = local.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.segmentation_pipeline_sa.email}"
}

# Allow the Dataflow Service Agent to launch workers using the pipeline service account.
# Without this, the service agent cannot attach the worker SA to Dataflow VMs.
resource "google_service_account_iam_member" "dataflow_service_agent_impersonation" {
  service_account_id = google_service_account.segmentation_pipeline_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:service-${local.project_number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}

# Allow the terraform-deployer to "act as" the segmentation pipeline service account.
# This is a critical pre-flight requirement for launching Dataflow jobs.
resource "google_service_account_iam_member" "terraform_deployer_impersonation" {
  count              = var.wif_service_account != null && var.wif_service_account != "" ? 1 : 0
  service_account_id = google_service_account.segmentation_pipeline_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.wif_service_account}"
}

# =============================================================================
# MONITORING
# =============================================================================

module "monitoring" {
  count  = var.enable_monitoring ? 1 : 0
  source = "./monitoring"

  environment             = var.environment
  notification_channel_id = var.notification_channel_id
}

