output "rules_api_url" {
  description = "The URL where the Rules API Service is hosted"
  value       = google_cloud_run_v2_service.rules_management.uri
}

output "rules_api_service_name" {
  description = "The exact name of the service (used by GitHub Actions and IAM)"
  value       = google_cloud_run_v2_service.rules_management.name
}

output "rules_api_sa_email" {
  description = "The email address of the rules management service account"
  value       = google_service_account.rules_management_sa.email
}
