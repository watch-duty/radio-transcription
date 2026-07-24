output "normalization_service_name" {
  description = "The name of the normalization Cloud Run service."
  value       = google_cloud_run_v2_service.normalization_service.name
}

output "normalization_service_uri" {
  description = "The URI of the normalization Cloud Run service."
  value       = google_cloud_run_v2_service.normalization_service.uri
}
