data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
}

resource "google_logging_metric" "pipeline_stage_count" {
  project     = local.project_id
  name        = "pipeline_stage_count"
  description = "Pipeline stage transitions (start, success, error) logged by serverless components."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND jsonPayload.event_type="pipeline_stage"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "stage"
      value_type  = "STRING"
      description = "The pipeline stage (e.g. normalization, transcription, evaluation, notification)."
    }
    labels {
      key         = "status"
      value_type  = "STRING"
      description = "The stage status (e.g. start, success, error)."
    }
  }

  label_extractors = {
    "stage"  = "EXTRACT(jsonPayload.stage)"
    "status" = "EXTRACT(jsonPayload.status)"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_logging_metric" "trace_propagation_failures" {
  project     = local.project_id
  name        = "pipeline_trace_propagation_failures"
  description = "Fires when trace context propagation fails and root spans are started by downstream services."
  filter      = <<-EOT
    (resource.type="cloud_run_revision" OR resource.type="dataflow_step")
    AND severity>=ERROR
    AND (textPayload:"Trace context propagation failure" OR jsonPayload.message:"Trace context propagation failure")
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
  }
}

resource "time_sleep" "wait_for_trace_metric" {
  depends_on = [
    google_logging_metric.trace_propagation_failures,
  ]
  create_duration = "120s"
}

resource "google_monitoring_alert_policy" "trace_propagation_failures" {
  project      = local.project_id
  display_name = "Trace Context Propagation Failure (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  depends_on = [time_sleep.wait_for_trace_metric]

  documentation {
    mime_type = "text/markdown"
    content   = "Trace context propagation failed in one of the pipeline services. A recent deployment or code change likely broke tracing context serialization/deserialization on the message queues (Pub/Sub) or API payload parameters.\n\n**Next Steps**:\n1. Search Cloud Logging for `textPayload:\"Trace context propagation failure\" OR jsonPayload.message:\"Trace context propagation failure\"` to identify the failing service.\n2. Check the most recent PRs/Deployments for that service to verify trace metadata extraction."
  }

  conditions {
    display_name = "Cloud Run trace propagation failures > 0"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.trace_propagation_failures.name}\""

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
    display_name = "Dataflow trace propagation failures > 0"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = "resource.type=\"dataflow_job\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.trace_propagation_failures.name}\""

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

  alert_strategy {
    auto_close = "86400s"
  }
}
