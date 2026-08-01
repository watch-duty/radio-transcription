data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
  deployer_iam_enabled = (
    var.deployer_sa_email != null
    && trimspace(var.deployer_sa_email) != ""
  )
}

# Allow the terraform-deployer service account to list and manage Dataflow jobs
resource "google_project_iam_member" "terraform_deployer_dataflow_developer" {
  project = local.project_id
  role    = "roles/dataflow.developer"
  member  = "serviceAccount:${var.deployer_sa_email}"
}

# Allow the terraform-deployer to manage Pub/Sub subscriptions (required to clean up stale tracking subscriptions during deploy)
resource "google_project_iam_member" "terraform_deployer_pubsub_admin" {
  project = local.project_id
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${var.deployer_sa_email}"
}

# Allow the terraform-deployer to read job error logs for failed-deploy diagnostics.
# deploy_dataflow.sh fetches a failed Dataflow job's error logs via `gcloud logging read`
# (logging.logEntries.list) to print the cause inline in the CI log; roles/dataflow.developer does not grant this.
resource "google_project_iam_member" "terraform_deployer_logging_viewer" {
  project = local.project_id
  role    = "roles/logging.viewer"
  member  = "serviceAccount:${var.deployer_sa_email}"
}

# Allow the terraform-deployer to manage Cloud Logging sinks.
# Required by the Feed Change Notifications route.
resource "google_project_iam_member" "terraform_deployer_logging_config_writer" {
  project = local.project_id
  role    = "roles/logging.configWriter"
  member  = "serviceAccount:${var.deployer_sa_email}"
}

# Allow the terraform-deployer service account to pull images for inspection during build
resource "google_project_iam_member" "terraform_deployer_artifact_reader" {
  project = local.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${var.deployer_sa_email}"
}

# Allow the terraform-deployer to set IAM policies on service accounts.
# Required to manage the dataflow_service_agent_impersonation binding.
resource "google_project_iam_member" "terraform_deployer_sa_admin" {
  project = local.project_id
  role    = "roles/iam.serviceAccountAdmin"
  member  = "serviceAccount:${var.deployer_sa_email}"
}

# Allow the terraform-deployer to read the operation_result file from the staging bucket.
# gcloud dataflow flex-template run polls this file to determine if the job launched successfully.
resource "google_storage_bucket_iam_member" "terraform_deployer_staging_reader" {
  count = var.dataflow_staging_bucket_name != null && var.dataflow_staging_bucket_name != "" ? 1 : 0

  bucket = var.dataflow_staging_bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${var.deployer_sa_email}"
}
