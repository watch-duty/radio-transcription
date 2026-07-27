output "transcription_service_name" {
  description = "The name of the transcription service"
  value       = google_cloud_run_v2_service.transcription_service.name
}

output "transcription_service_url" {
  description = "The URL of the transcription service"
  value       = google_cloud_run_v2_service.transcription_service.uri
}
