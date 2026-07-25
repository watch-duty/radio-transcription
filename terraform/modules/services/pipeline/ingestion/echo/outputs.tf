output "echo_recordings_bucket_name" {
  description = "The name of the Echo recordings bucket."
  value       = module.echo_recordings_bucket.name
}

output "echo_recordings_uploader_sa_email" {
  description = "The email of the Echo recordings uploader service account."
  value       = google_service_account.echo_recordings_uploader.email
}

output "echo_recordings_uploader_hmac_access_id" {
  description = "The HMAC access ID for uploading Echo recordings."
  value       = google_storage_hmac_key.echo_recordings_uploader.access_id
}

output "echo_recordings_uploader_hmac_secret" {
  description = "The HMAC secret for uploading Echo recordings."
  value       = google_storage_hmac_key.echo_recordings_uploader.secret
  sensitive   = true
}

output "echo_ingestion_service_name" {
  description = "The name of the deployed echo ingestion Cloud Run service."
  value       = google_cloud_run_v2_service.echo_ingestion.name
}

output "echo_ingestion_sa_email" {
  description = "The email of the Echo ingestion service account."
  value       = google_service_account.echo_ingestion.email
}
