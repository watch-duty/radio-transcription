output "feeds_api_url" {
  description = "The URL where the Feed Store Service is hosted"
  value       = google_cloud_run_v2_service.feed_store.uri
}

output "feed_store_service_name" {
  description = "The name of the service"
  value       = google_cloud_run_v2_service.feed_store.name
}

output "feed_store_sa_email" {
  description = "The email address of the Feed Store service account"
  value       = google_service_account.feed_store_sa.email
}
