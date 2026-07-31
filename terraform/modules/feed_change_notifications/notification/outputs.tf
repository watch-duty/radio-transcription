output "push_subscription_name" {
  description = "The name of the main Pub/Sub push subscription."
  value       = google_pubsub_subscription.feed_change_notifications_push.name
}

output "dlq_subscription_name" {
  description = "The name of the DLQ Pub/Sub subscription."
  value       = google_pubsub_subscription.feed_change_notifications_dlq.name
}
