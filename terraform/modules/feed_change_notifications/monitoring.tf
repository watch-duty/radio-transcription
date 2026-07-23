# =============================================================================
# FEED CHANGE NOTIFICATIONS MONITORING
# =============================================================================

resource "google_logging_metric" "webhook_retryable_failures" {
  project     = local.project_id
  name        = "feed_change_webhook_retryable_failures"
  description = "Retryable Feed Change Notifications relay delivery failures."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND resource.labels.service_name="${module.webhook_relay.feed_change_webhook_service_name}"
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
    AND resource.labels.service_name="${module.webhook_relay.feed_change_webhook_service_name}"
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

resource "time_sleep" "wait_for_webhook_metrics" {
  count = var.enabled ? 1 : 0

  depends_on = [
    google_logging_metric.webhook_retryable_failures,
    google_logging_metric.webhook_permanent_failures,
  ]

  create_duration = "120s"
}

resource "google_monitoring_alert_policy" "delivery_health" {
  count = var.enabled ? 1 : 0

  project      = local.project_id
  display_name = "Feed Change Notifications Delivery Health (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.slack_critical_notification_channel_id != null ? [var.slack_critical_notification_channel_id] : []

  depends_on = [time_sleep.wait_for_webhook_metrics]

  documentation {
    mime_type = "text/markdown"
    content   = "Feed Change Notifications delivery needs operator attention. Inspect `${module.webhook_relay.feed_change_webhook_service_name}` relay logs, Pub/Sub subscription health, and destination webhook responses for retryable or permanent delivery failures."
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

      filter = "resource.type=\"pubsub_subscription\" AND metric.type=\"pubsub.googleapis.com/subscription/dead_letter_message_count\" AND resource.labels.subscription_id=\"${google_pubsub_subscription.feed_change_notifications_push[0].name}\""

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

      filter = "resource.type=\"pubsub_subscription\" AND metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.labels.subscription_id=\"${google_pubsub_subscription.feed_change_notifications_dlq[0].name}\""

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
