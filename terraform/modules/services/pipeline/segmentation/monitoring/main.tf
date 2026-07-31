data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
}

resource "google_monitoring_alert_policy" "dataflow_lag_alert" {
  project      = local.project_id
  display_name = "Dataflow Data Lag > 1 Hour"
  combiner     = "OR"
  conditions {
    display_name = "Data Freshness Threshold"
    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "60s"
      filter          = "resource.type = \"dataflow_job\" AND metric.type = \"dataflow.googleapis.com/job/system_lag\""
      threshold_value = "3600"
      trigger {
        count = "1"
      }
    }
  }
  user_labels = {
    severity = "warning"
  }
}

resource "google_monitoring_alert_policy" "dataflow_input_backpressure_alert" {
  project = local.project_id
  for_each = toset([
    "continuous-audio-sub-${var.environment}"
  ])

  display_name = "Dataflow Input Stream Backpressure - ${each.value} (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "Backlog processing queue message retention limits exceeded for subscription stream queue ${each.value} consumed by streaming Dataflow workers. Validate runner operations, database performance profiles, or runtime tasks."
  }

  conditions {
    display_name = "High Backlog Messages in Streaming Input Subscription"
    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      threshold_value = 15000

      filter = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\" AND resource.labels.subscription_id = \"${each.value}\""

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      trigger {
        count = 1
      }
    }
  }

  conditions {
    display_name = "Oldest Message Processing Timeout on Streaming Queue"
    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      threshold_value = 1200

      filter = "resource.type = \"pubsub_subscription\" AND metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age\" AND resource.labels.subscription_id = \"${each.value}\""

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }

      trigger {
        count = 1
      }
    }
  }

  user_labels = {
    severity  = "warning"
    subsystem = "dataflow"
  }
}
