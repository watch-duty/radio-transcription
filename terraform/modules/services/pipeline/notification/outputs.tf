output "notification_service_name" {
  description = "The name of the deployed notification pipeline Cloud Run service."
  value       = google_cloud_run_v2_service.notification_pipeline.name
}

output "notification_service_uri" {
  description = "The URI of the deployed notification pipeline Cloud Run service."
  value       = google_cloud_run_v2_service.notification_pipeline.uri
}

output "alert_notification_subscription_name" {
  description = "The name of the Pub/Sub push subscription for notifications."
  value       = google_pubsub_subscription.alert_notification_pubsub_subscription.name
}
