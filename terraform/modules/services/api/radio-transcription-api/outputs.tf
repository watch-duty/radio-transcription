output "radio_transcription_api_name" {
  description = "The name of the deployed radio transcription API Cloud Run service."
  value       = google_cloud_run_v2_service.radio_transcription_api.name
}

output "radio_transcription_api_uri" {
  description = "The URI of the deployed radio transcription API Cloud Run service."
  value       = google_cloud_run_v2_service.radio_transcription_api.uri
}

output "radio_transcription_api_url" {
  description = "The URL of the API Gateway."
  value       = "https://${google_api_gateway_gateway.radio_transcription_api_gw_gateway.default_hostname}"
}
