data "google_project" "project" {}

locals {
  project_id           = data.google_project.project.project_id
  project_number       = data.google_project.project.number
  relay_push_endpoint  = "${var.relay_service_url}/pubsub/feed-change-notifications"
  pubsub_service_agent = "service-${local.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# =============================================================================
# PUB/SUB TOPICS & SUBSCRIPTIONS
# =============================================================================

resource "google_pubsub_topic" "feed_change_notifications" {
  project = local.project_id
  name    = "feed-change-notifications-${var.environment}"
}

resource "google_pubsub_topic" "feed_change_notifications_dlq" {
  project = local.project_id
  name    = "feed-change-notifications-dlq-${var.environment}"
}

resource "google_pubsub_subscription" "feed_change_notifications_dlq" {
  project = local.project_id
  name    = "feed-change-notifications-dlq-subscription-${var.environment}"
  topic   = google_pubsub_topic.feed_change_notifications_dlq.name

  message_retention_duration = "604800s" # 7 days
}

# Route Feed Change Notifications structured logs to the dedicated Pub/Sub topic.
resource "google_logging_project_sink" "feed_change_notifications" {
  project = local.project_id
  name    = "feed-change-notifications-route-${var.environment}"

  destination = "pubsub.googleapis.com/${google_pubsub_topic.feed_change_notifications.id}"
  filter      = <<-EOT
    jsonPayload.event_type="radio_transcription.feed_change_notification"
    AND jsonPayload.schema_version=1
  EOT

  unique_writer_identity = true
}

# Allow only this sink writer to publish to the notification topic.
resource "google_pubsub_topic_iam_member" "sink_publisher" {
  project = local.project_id
  topic   = google_pubsub_topic.feed_change_notifications.name
  role    = "roles/pubsub.publisher"
  member  = google_logging_project_sink.feed_change_notifications.writer_identity
}

# -----------------------------------------------------------------------------
# Pub/Sub Push Invoker IAM
# -----------------------------------------------------------------------------

# Service account used only by Pub/Sub push delivery to the feed change relay.
resource "google_service_account" "push_invoker" {
  account_id   = "feed-change-push-${var.environment}"
  display_name = "Feed Change Notifications Pub/Sub Push Invoker"
}

# Allow Pub/Sub-authenticated delivery to invoke the relay Cloud Run service.
resource "google_cloud_run_v2_service_iam_member" "push_invoker_run_invoker" {
  project  = local.project_id
  location = var.region
  name     = var.relay_service_name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.push_invoker.email}"
}

# Allow the Terraform deployer to attach the push invoker service account to the subscription OIDC config.
resource "google_service_account_iam_member" "deployer_push_invoker_user" {
  count = var.deployer_service_account_email != null && var.deployer_service_account_email != "" ? 1 : 0

  service_account_id = google_service_account.push_invoker.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.deployer_service_account_email}"
}

# Allow Pub/Sub to mint OIDC tokens for the push invoker service account.
resource "google_service_account_iam_member" "pubsub_token_creator" {
  service_account_id = google_service_account.push_invoker.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${local.pubsub_service_agent}"
}

# Allow Pub/Sub to publish failed delivery attempts to the route DLQ.
resource "google_pubsub_topic_iam_member" "dlq_publisher" {
  project = local.project_id
  topic   = google_pubsub_topic.feed_change_notifications_dlq.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${local.pubsub_service_agent}"
}

# Push routed Feed Change Notifications log entries to the relay service.
resource "google_pubsub_subscription" "feed_change_notifications_push" {
  project = local.project_id
  name    = "feed-change-notifications-subscription-${var.environment}"
  topic   = google_pubsub_topic.feed_change_notifications.id

  push_config {
    push_endpoint = local.relay_push_endpoint

    oidc_token {
      service_account_email = google_service_account.push_invoker.email
      audience              = var.relay_service_url
    }
  }

  # Covers three 15-second destination webhook attempts plus jitter and overhead.
  ack_deadline_seconds = 60

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.feed_change_notifications_dlq.id
    max_delivery_attempts = 10
  }

  depends_on = [
    google_cloud_run_v2_service_iam_member.push_invoker_run_invoker,
    google_service_account_iam_member.deployer_push_invoker_user,
    google_service_account_iam_member.pubsub_token_creator,
    google_pubsub_topic_iam_member.dlq_publisher,
  ]
}

# Allows Pub/Sub to pull and acknowledge messages that will be put on the DLQ.
resource "google_pubsub_subscription_iam_member" "source_subscription_subscriber" {
  project      = local.project_id
  subscription = google_pubsub_subscription.feed_change_notifications_push.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${local.pubsub_service_agent}"
}