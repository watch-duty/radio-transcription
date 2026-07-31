output "service_name" {
  description = "Name of the Publisher Cloud Run v2 service (e.g. oldest-feed-publisher-prod). Useful for ad-hoc gcloud run services describe and for referencing the service from a future autoscaler-related resource if needed."
  value       = google_cloud_run_v2_service.oldest_feed_publisher.name
}

output "service_uri" {
  description = "HTTPS URI of the Publisher Cloud Run v2 service. Useful for manual curl-via-IAP testing and for propagating to downstream observability dashboards."
  value       = google_cloud_run_v2_service.oldest_feed_publisher.uri
}

output "publisher_sa_email" {
  description = "Email of the Publisher Cloud Run service account (oldest-feed-publisher-<env>). Useful for cross-module IAM grants if a future workstream needs to grant the Publisher new permissions."
  value       = google_service_account.oldest_feed_publisher.email
}

output "scheduler_sa_email" {
  description = "Email of the Cloud Scheduler service account (oldest-feed-pub-scheduler-<env>) that mints OIDC tokens to invoke the Publisher service."
  value       = google_service_account.oldest_feed_publisher_scheduler.email
}
