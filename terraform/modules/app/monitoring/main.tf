data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
  # Sizing model/inputs hash to version the metric name when its schema/labels change.
  # This prevents GCP 400 alerting policy locks during updates by forcing a new metric
  # name, updating the alert policy to point to it, and then safely reaping the old one.
  e2e_latency_metric_hash = substr(sha256(join("-", [
    "feed_type"
  ])), 0, 8)
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

resource "google_logging_metric" "transcription_e2e_latency_ms" {
  project     = local.project_id
  name        = "transcription_e2e_latency_${local.e2e_latency_metric_hash}_ms"
  description = "End-to-end processing latency from ingestion to evaluation."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND jsonPayload.event_type="e2e_latency"
    AND jsonPayload.latency_ms >= 0
  EOT

  value_extractor = "EXTRACT(jsonPayload.latency_ms)"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "ms"
    labels {
      key         = "feed_type"
      value_type  = "STRING"
      description = "The type of the feed."
    }
  }

  label_extractors = {
    "feed_type" = "EXTRACT(jsonPayload.feed_type)"
  }

  bucket_options {
    explicit_buckets {
      bounds = [1000, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 240000, 360000, 480000, 540000, 660000, 780000, 900000, 1200000, 1500000, 1800000]
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Log-based metrics are eventually consistent in GCP. 
# We introduce a delay to ensure the metric descriptor is fully propagated 
# before Terraform attempts to attach an alert policy to it, avoiding "Metric not found" errors.
resource "time_sleep" "wait_for_latency_metric" {
  triggers = {
    metric_name = google_logging_metric.transcription_e2e_latency_ms.name
  }
  create_duration = "120s"
}

resource "google_monitoring_alert_policy" "transcription_e2e_latency" {
  project      = local.project_id
  display_name = "E2E Latency P95 > 5m (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  depends_on = [time_sleep.wait_for_latency_metric]

  documentation {
    mime_type = "text/markdown"
    content   = "End-to-end processing latency (from ingestion to evaluation) has exceeded 5 minutes (300,000 ms) for 5 minutes sustained.\n\n**Next Steps**:\n1. Check the System Health Overview dashboard to see the E2E latency p95 / p99 trends.\n2. Investigate components in the pipeline (Ingestion MIG, Transcription Cloud Run, Evaluation Cloud Run).\n3. Check for Dataflow system lag or backlog spikes."
  }

  conditions {
    display_name = "E2E Latency P95 > 5m sustained 5m"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      threshold_value = 300000

      filter = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.transcription_e2e_latency_ms.name}\""

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_PERCENTILE_95"
        cross_series_reducer = "REDUCE_MAX"
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
    severity = "warning"
  }
}

# =============================================================================
# TRANSCRIPTION PIPELINE STAGE ALERTS
# =============================================================================

# Log-based metrics are eventually consistent in GCP.
# We introduce a delay to ensure the metric descriptor is fully propagated
# before Terraform attempts to attach an alert policy to it.
resource "time_sleep" "wait_for_pipeline_metric" {
  depends_on = [
    google_logging_metric.pipeline_stage_count,
  ]
  create_duration = "120s"
}

resource "google_monitoring_alert_policy" "transcription_error_rate" {
  project      = local.project_id
  display_name = "Transcription Error Rate Alert (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  depends_on = [time_sleep.wait_for_pipeline_metric]

  documentation {
    mime_type = "text/markdown"
    content   = "Transcription pipeline failures detected.\n\n**Details**:\nEither the error rate exceeds 1% of total transcription attempts over a 5-minute rolling window, or raw errors have exceeded 5 within 5 minutes.\n\n**Next Steps**:\n1. Open Cloud Logging and filter by `resource.type=\"cloud_run_revision\" AND jsonPayload.stage=\"transcription\" AND jsonPayload.status=\"error\"`.\n2. Check if Speech-to-Text API quota has been exhausted (e.g., 429 resource exhausted error).\n3. Investigate the transcription container logs for tracebacks or authentication/network failures."
  }

  conditions {
    display_name = "Transcription error rate exceeds 1% (rolling 5m)"

    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_stage_count.name}\" AND metric.labels.stage=\"transcription_status\" AND metric.labels.status=\"permanent_error\""
      comparison      = "COMPARISON_GT"
      duration        = "120s"
      threshold_value = 0.01

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
      }

      denominator_filter = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_stage_count.name}\" AND metric.labels.stage=\"transcription_status\" AND metric.labels.status=\"attempts\""

      denominator_aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  conditions {
    display_name = "Transcription raw errors exceed 5 (rolling 5m)"

    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_stage_count.name}\" AND metric.labels.stage=\"transcription_status\" AND metric.labels.status=\"permanent_error\""
      comparison      = "COMPARISON_GT"
      duration        = "120s"
      threshold_value = 5

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_SUM"
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

resource "google_monitoring_alert_policy" "transcription_other_error_rate" {
  project      = local.project_id
  display_name = "Transcription Transient/Policy Block Rate Alert (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  depends_on = [time_sleep.wait_for_pipeline_metric]

  documentation {
    mime_type = "text/markdown"
    content   = "High rate of transient or policy blocked transcription failures detected.\n\n**Details**:\nEither the transient/policy blocked rate exceeds 5% of total transcription attempts over a 5-minute rolling window, or raw occurrences have exceeded 15 within 5 minutes.\n\n**Next Steps**:\n1. Check for downstream Gemini API instability (500s) or rate limits (429s).\n2. If policy blocked, check if safety filters are too aggressive."
  }

  conditions {
    display_name = "Transient/Policy Block rate exceeds 5% (rolling 5m)"

    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_stage_count.name}\" AND metric.labels.stage=\"transcription_status\" AND metric.labels.status=one_of(\"transient_error\", \"policy_blocked\")"
      comparison      = "COMPARISON_GT"
      duration        = "120s"
      threshold_value = 0.05

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
      }

      denominator_filter = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_stage_count.name}\" AND metric.labels.stage=\"transcription_status\" AND metric.labels.status=\"attempts\""

      denominator_aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  conditions {
    display_name = "Transient/Policy Block raw errors exceed 15 (rolling 5m)"

    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"logging.googleapis.com/user/${google_logging_metric.pipeline_stage_count.name}\" AND metric.labels.stage=\"transcription_status\" AND metric.labels.status=one_of(\"transient_error\", \"policy_blocked\")"
      comparison      = "COMPARISON_GT"
      duration        = "120s"
      threshold_value = 15

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_SUM"
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
    severity = "warning"
  }
}

resource "google_monitoring_dashboard" "system_health_overview" {
  project = local.project_id
  dashboard_json = templatefile("${path.module}/dashboards/system_health_overview.json.tftpl", {
    environment             = var.environment
    e2e_latency_metric_name = google_logging_metric.transcription_e2e_latency_ms.name
  })
}


