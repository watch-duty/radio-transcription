output "segmentation_pipeline_sa_email" {
  description = "The email of the segmentation pipeline service account"
  value       = google_service_account.segmentation_pipeline_sa.email
}
