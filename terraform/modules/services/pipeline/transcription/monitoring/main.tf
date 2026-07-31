data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
}

resource "google_monitoring_alert_policy" "dlq_spike_alert" {
  project      = local.project_id
  display_name = "DLQ Volume Spike"
  combiner     = "OR"
  conditions {
    display_name = "High Message Rate on DLQ Topic"
    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      filter          = "resource.type = \"pubsub_topic\" AND resource.labels.topic_id = \"transcribed-audio-dlq-${var.environment}\" AND metric.type = \"pubsub.googleapis.com/topic/send_request_count\""
      threshold_value = "100"
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
      trigger {
        count = "1"
      }
    }
  }
  conditions {
    display_name = "High Message Rate on Segmentation DLQ Topic"
    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      filter          = "resource.type = \"pubsub_topic\" AND resource.labels.topic_id = \"segmented-audio-claims-dlq-${var.environment}\" AND metric.type = \"pubsub.googleapis.com/topic/send_request_count\""
      threshold_value = "100"
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
      trigger {
        count = "1"
      }
    }
  }
  user_labels = {
    severity = "warning"
  }
}
