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

resource "google_monitoring_alert_policy" "gcp_api_quota_exceeded" {
  project      = local.project_id
  display_name = "GCP API Quota Exceeded (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "One or more GCP API quotas have been exceeded for Speech-to-Text V2 or Vertex AI APIs.\n\n**Next Steps**:\n1. Check the Google Cloud Console IAM & Admin Quotas page for the project.\n2. Review current traffic or request volumes for Speech-to-Text (`speech.googleapis.com`) and Vertex AI (`aiplatform.googleapis.com`) APIs.\n3. Request a quota increase if this is a legitimate usage spike, or optimize/throttle API consumption."
  }

  conditions {
    display_name = "API Quota Exceeded (Speech-to-Text or Vertex AI)"

    condition_threshold {
      filter          = "resource.type=\"consumer_quota\" AND metric.type=\"serviceruntime.googleapis.com/quota/exceeded\" AND (resource.labels.service=\"speech.googleapis.com\" OR resource.labels.service=\"aiplatform.googleapis.com\")"
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_COUNT_TRUE"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close = "86400s"
  }

  user_labels = {
    severity = "critical"
  }
}

