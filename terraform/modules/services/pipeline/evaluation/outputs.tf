output "evaluation_service_name" {
  description = "The name of the deployed evaluation pipeline Cloud Run service."
  value       = google_cloud_run_v2_service.evaluation_pipeline.name
}

output "evaluation_service_uri" {
  description = "The URI of the deployed evaluation pipeline Cloud Run service."
  value       = google_cloud_run_v2_service.evaluation_pipeline.uri
}
