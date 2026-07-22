output "audio_segments_api_service_url" {
  description = "The URL where the Audio Segments API Service is hosted"
  value       = google_cloud_run_v2_service.audio_segments_api.uri
}

output "audio_segments_api_service_name" {
  description = "The name of the service"
  value       = google_cloud_run_v2_service.audio_segments_api.name
}

output "audio_segments_api_sa_email" {
  description = "The email address of the Audio Segments API service account"
  value       = google_service_account.audio_segments_api_sa.email
}
