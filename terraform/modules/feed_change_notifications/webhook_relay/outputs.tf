output "feed_change_webhook_service_name" {
  description = "The deployed Feed Change Webhook relay Cloud Run service name."
  value       = google_cloud_run_v2_service.feed_change_webhook.name
}

output "feed_change_webhook_service_url" {
  description = "The deployed Feed Change Webhook relay Cloud Run service URL."
  value       = google_cloud_run_v2_service.feed_change_webhook.uri
}
