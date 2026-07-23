data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
}

# =============================================================================
# FEED CHANGE NOTIFICATIONS MONITORING METRICS
# =============================================================================

resource "google_logging_metric" "webhook_retryable_failures" {
  project     = local.project_id
  name        = "feed_change_webhook_retryable_failures"
  description = "Retryable Feed Change Notifications relay delivery failures."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND resource.labels.service_name="${var.relay_service_name}"
    AND jsonPayload.relay_event="feed_change_webhook_delivery"
    AND jsonPayload.retryable=true
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_logging_metric" "webhook_permanent_failures" {
  project     = local.project_id
  name        = "feed_change_webhook_permanent_failures"
  description = "Permanent or configuration Feed Change Notifications relay failures."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND resource.labels.service_name="${var.relay_service_name}"
    AND (
      (jsonPayload.relay_event="feed_change_webhook_delivery" AND jsonPayload.retryable=false)
      OR jsonPayload.relay_event="feed_change_webhook_invalid_pubsub_message"
      OR jsonPayload.relay_event="feed_change_webhook_client_not_initialized"
      OR jsonPayload.relay_event="feed_change_webhook_unhandled_delivery_error"
    )
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# =============================================================================
# ALERT POLICY
# =============================================================================

resource "time_sleep" "wait_for_webhook_metrics" {
  create_duration = "120s"

  triggers = {
    retryable_metric = google_logging_metric.webhook_retryable_failures.name
    permanent_metric = google_logging_metric.webhook_permanent_failures.name
  }
}

resource "google_monitoring_alert_policy" "delivery_health" {
  project      = local.project_id
  display_name = "Feed Change Notifications Delivery Health (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.slack_critical_notification_channel_id != null ? [var.slack_critical_notification_channel_id] : []

  depends_on = [time_sleep.wait_for_webhook_metrics]

  documentation {
    mime_type = "text/markdown"
    content   = "Feed Change Notifications delivery needs operator attention. Inspect `${var.relay_service_name}` relay logs, Pub/Sub subscription health, and destination webhook responses for retryable or permanent delivery failures."
  }

  conditions {
    display_name = "Retryable relay failures > 5 in 5m"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 5

      filter = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.webhook_retryable_failures.name}\""

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  conditions {
    display_name = "Permanent or configuration relay failures > 0"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.webhook_permanent_failures.name}\""

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  conditions {
    display_name = "Source subscription forwarded messages to DLQ"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = "resource.type=\"pubsub_subscription\" AND metric.type=\"pubsub.googleapis.com/subscription/dead_letter_message_count\" AND resource.labels.subscription_id=\"${var.push_subscription_name}\""

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  conditions {
    display_name = "DLQ inspection subscription backlog > 0"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = "resource.type=\"pubsub_subscription\" AND metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.labels.subscription_id=\"${var.dlq_subscription_name}\""

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "1800s"
  }

  user_labels = {
    severity  = "warning"
    subsystem = "feed-change-notifications"
  }
}
