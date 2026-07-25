locals {
  metric_name_suffix         = "_${var.environment}"
  resolved_echo_service_name = coalesce(var.echo_service_name, "echo-audio-ingestion-${var.environment}")

  download_source_types_one_of = "one_of(${join(", ", [for s in var.download_source_types : "\"${s}\""])})"

  # Filter strings are built with join(" AND ", [...]) — not heredocs — so they land as
  # single-line strings. The Terraform provider for google_monitoring_slo does not
  # whitespace-normalize filter fields on read-back (see provider issue #7425 for the
  # sibling alert-policy resource), so heredoc-rendered multi-line filters would cause
  # perpetual plan diffs after apply. This also matches the precedent in the sibling
  # broadcastify_credential_rotation/ sub-module.
  download_good_filter = join(" AND ", [
    "metric.type=\"logging.googleapis.com/user/${google_logging_metric.chunk_ingested.name}\"",
    "resource.type=\"gce_instance\"",
    "metric.labels.source_type=${local.download_source_types_one_of}",
  ])

  # Mirror source_type from download_good_filter — good and bad MUST share the same universe,
  # or a failure outside download_source_types would depress the SLO with no offsetting good.
  download_bad_filter = join(" AND ", [
    "metric.type=\"logging.googleapis.com/user/${google_logging_metric.call_download_failed.name}\"",
    "resource.type=\"gce_instance\"",
    "metric.labels.source_type=${local.download_source_types_one_of}",
  ])

  echo_good_filter = join(" AND ", [
    "metric.type=\"run.googleapis.com/request_count\"",
    "resource.type=\"cloud_run_revision\"",
    "resource.labels.service_name=\"${local.resolved_echo_service_name}\"",
    "metric.labels.response_code_class=\"2xx\"",
  ])

  echo_bad_filter = join(" AND ", [
    "metric.type=\"run.googleapis.com/request_count\"",
    "resource.type=\"cloud_run_revision\"",
    "resource.labels.service_name=\"${local.resolved_echo_service_name}\"",
    "metric.labels.response_code_class=\"5xx\"",
  ])
}

resource "google_logging_metric" "chunk_ingested" {
  project     = var.project_id
  name        = "${var.log_metric_name_prefix}_chunk_ingested${local.metric_name_suffix}"
  description = "Successful chunk-ingest events from the ingestion MIG. Drives the download SLO's good filter."
  filter      = <<-EOT
    resource.type="gce_instance"
    AND labels.python_logger=~"backend.pipeline.ingestion"
    AND jsonPayload.event_type="chunk_ingested"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    # source_type-only labeling — 12K feeds x 25 VMs with feed_id would exceed Cloud Monitoring's ~30K series soft limit.
    labels {
      key         = "source_type"
      value_type  = "STRING"
      description = "Upstream source type (bcfy_feeds, openmhz, bcfy_calls)."
    }
  }

  label_extractors = {
    "source_type" = "EXTRACT(jsonPayload.source_type)"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_logging_metric" "chunk_latency" {
  project     = var.project_id
  name        = "${var.log_metric_name_prefix}_chunk_latency${local.metric_name_suffix}"
  description = "Per-chunk processing latency distribution from the ingestion MIG. Drives per-source latency alerting in the follow-up alerts project."
  # Clock-skew negatives would corrupt the distribution; filter at ingest.
  filter = <<-EOT
    resource.type="gce_instance"
    AND labels.python_logger=~"backend.pipeline.ingestion"
    AND jsonPayload.event_type="chunk_ingested"
    AND jsonPayload.processing_latency_sec >= 0
  EOT

  value_extractor = "EXTRACT(jsonPayload.processing_latency_sec)"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "s"
    labels {
      key         = "source_type"
      value_type  = "STRING"
      description = "Upstream source type."
    }
  }

  label_extractors = {
    "source_type" = "EXTRACT(jsonPayload.source_type)"
  }

  bucket_options {
    # Hand-picked bounds for +-7.5s resolution near the 30s/60s alert thresholds. Changing requires descriptor delete+recreate.
    explicit_buckets {
      bounds = [1, 2, 5, 10, 15, 30, 45, 60, 90, 120, 180]
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_logging_metric" "call_download_failed" {
  project     = var.project_id
  name        = "${var.log_metric_name_prefix}_call_download_failed${local.metric_name_suffix}"
  description = "Audio download failures from polling collectors. Drives the download SLO's bad filter."
  filter      = <<-EOT
    resource.type="gce_instance"
    AND labels.python_logger=~"backend.pipeline.ingestion"
    AND jsonPayload.event_type="call_download_failed"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "source_type"
      value_type  = "STRING"
      description = "Upstream source type."
    }
  }

  label_extractors = {
    "source_type" = "EXTRACT(jsonPayload.source_type)"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_logging_metric" "feed_quarantined" {
  project     = var.project_id
  name        = "${var.log_metric_name_prefix}_feed_quarantine_with_labels${local.metric_name_suffix}"
  description = "Feed quarantine events. Drives the quarantine alert in the follow-up alerts project."
  filter      = <<-EOT
    resource.type="gce_instance"
    AND labels.python_logger=~"backend.pipeline.ingestion"
    AND jsonPayload.event_type="feed_quarantined"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "source_type"
      value_type  = "STRING"
      description = "Upstream source type."
    }
    labels {
      key         = "feed_id"
      value_type  = "STRING"
      description = "The UUID of the feed that was quarantined."
    }
    labels {
      key         = "feed_name"
      value_type  = "STRING"
      description = "The name of the feed that was quarantined."
    }
  }

  label_extractors = {
    "source_type" = "EXTRACT(jsonPayload.source_type)"
    "feed_id"     = "EXTRACT(jsonPayload.feed_id)"
    "feed_name"   = "EXTRACT(jsonPayload.feed_name)"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_logging_metric" "collector_log_activity" {
  project     = var.project_id
  name        = "${var.log_metric_name_prefix}_collector_log_activity${local.metric_name_suffix}"
  description = "Any log emission from the ingestion logger. Drives the fleet-silent alert in the follow-up alerts project. Unlabeled on purpose (fleet aggregate)."
  filter      = <<-EOT
    resource.type="gce_instance"
    AND labels.python_logger=~"backend.pipeline.ingestion"
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

resource "google_monitoring_custom_service" "ingestion" {
  project      = var.project_id
  service_id   = "ingestion-${var.environment}"
  display_name = "Audio ingestion (MIG fleet) — ${var.environment}"
}

resource "google_monitoring_service" "echo" {
  project      = var.project_id
  service_id   = "echo-${var.environment}"
  display_name = "Echo audio ingestion (Cloud Run) — ${var.environment}"

  basic_service {
    service_type = "CLOUD_RUN"
    service_labels = {
      service_name = local.resolved_echo_service_name
      location     = var.region
    }
  }
}

resource "google_monitoring_slo" "download_success" {
  project             = var.project_id
  service             = google_monitoring_custom_service.ingestion.service_id
  slo_id              = "download-success-${var.environment}"
  display_name        = "Download success rate — ${var.environment}"
  goal                = var.download_slo_target
  rolling_period_days = 28

  request_based_sli {
    good_total_ratio {
      # chunk_ingested fires after GCS + Pub/Sub + bookmark, slightly undercounting successful downloads. Negligible due to 3x GCS retry.
      good_service_filter = local.download_good_filter
      bad_service_filter  = local.download_bad_filter
    }
  }
}

resource "google_monitoring_slo" "echo_availability" {
  project             = var.project_id
  service             = google_monitoring_service.echo.service_id
  slo_id              = "echo-availability-${var.environment}"
  display_name        = "Echo availability — ${var.environment}"
  goal                = var.echo_slo_target
  rolling_period_days = 28

  request_based_sli {
    good_total_ratio {
      good_service_filter = local.echo_good_filter
      bad_service_filter  = local.echo_bad_filter
    }
  }
}

# =============================================================================
# OPERATIONAL INVARIANT — Slack bot membership (VAL-04)
# =============================================================================
#
# All alert policies below set
#   notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []
# to route to the Slack channel created at
#   terraform/environments/prod/main.tf (resource: google_monitoring_notification_channel.slack_critical)
#
# CRITICAL: GCP does NOT enforce that the bot identified by
# SLACK_AUTH_TOKEN is a member of #bots-radio-transcription-alerts.
# Slack rejects message delivery SILENTLY if the bot is absent — the
# alert fires in the GCP console but no Slack message arrives.
#
# To verify bot membership:
#   1. In Slack, type `/who is in this channel?` in #bots-radio-transcription-alerts
#   2. The bot user (named after the workspace's GCP integration) must appear.
#   3. If absent: run `/invite @<bot-user>` then re-test via:
#         CHANNEL_ID=$(terraform -chdir=terraform/environments/prod \
#                       output -raw slack_critical_notification_channel_id)
#         gcloud alpha monitoring channels test --channel="$CHANNEL_ID" \
#                       --project=<prod-project-id>
#
# Symptoms of bot-not-in-channel failure:
#   - GCP shows alert as OPEN with notificationChannels populated
#   - `terraform output -raw slack_critical_notification_channel_id` is set
#   - No Slack message arrives within 30s of alert firing
#   - GCP audit log shows notificationChannel.sendNotification succeeded
#     but Slack API response payload contains "channel_not_found" / "not_in_channel"
#
# Recovery: re-invite the bot. NO Terraform changes required.
# =============================================================================

# =============================================================================
# SLO BURN-RATE ALERTS (ALERT-01, ALERT-02 — dual-window 6×)
# =============================================================================

resource "google_monitoring_alert_policy" "download_burn_rate" {
  project      = var.project_id
  display_name = "Download SLO burn-rate (${var.environment})"
  combiner     = "AND"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "Sustained breach of the download_success SLO. Both the 30-min fast-burn AND the 6-h slow-burn windows exceed 6× error-budget burn rate simultaneously, indicating real customer-affecting download failures (not a transient blip). Investigate upstream Broadcastify rate-limits, CDN health, and AlloyDB bookmark contention before silencing."
  }

  conditions {
    display_name = "Fast burn (30 min, 6× burn rate)"

    condition_threshold {
      filter = join(" AND ", [
        "select_slo_burn_rate(\"${google_monitoring_slo.download_success.name}\", \"1800s\")",
      ])

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 6.0

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
    display_name = "Slow burn (6 h, 6× burn rate)"

    condition_threshold {
      filter = join(" AND ", [
        "select_slo_burn_rate(\"${google_monitoring_slo.download_success.name}\", \"21600s\")",
      ])

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 6.0

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    # >= 2× fast-burn window (1800s) per Google SRE convention so the incident
    # doesn't auto-close before the on-call has time to acknowledge.
    auto_close = "3600s"
  }
}

resource "google_monitoring_alert_policy" "echo_burn_rate" {
  project      = var.project_id
  display_name = "Echo SLO burn-rate (${var.environment})"
  combiner     = "AND"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "Sustained breach of the echo_availability SLO. Both the 30-min fast-burn AND the 6-h slow-burn windows exceed 6× error-budget burn rate simultaneously, indicating real customer-affecting Cloud Run 5xx responses (not a transient blip). Investigate Cloud Run revision health, GCS upload errors, and Pub/Sub publish failures before silencing."
  }

  conditions {
    display_name = "Fast burn (30 min, 6× burn rate)"

    condition_threshold {
      filter = join(" AND ", [
        "select_slo_burn_rate(\"${google_monitoring_slo.echo_availability.name}\", \"1800s\")",
      ])

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 6.0

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
    display_name = "Slow burn (6 h, 6× burn rate)"

    condition_threshold {
      filter = join(" AND ", [
        "select_slo_burn_rate(\"${google_monitoring_slo.echo_availability.name}\", \"21600s\")",
      ])

      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 6.0

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MEAN"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    # >= 2× fast-burn window (1800s) per Google SRE convention so the incident
    # doesn't auto-close before the on-call has time to acknowledge.
    auto_close = "3600s"
  }
}

# =============================================================================
# HEALTH SIGNAL ALERTS (ALERT-03 quarantine, ALERT-04 fleet-silent)
# =============================================================================

resource "google_monitoring_alert_policy" "feed_quarantine" {
  project      = var.project_id
  display_name = "Feed quarantine event (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "A feed has been quarantined by the ingestion pipeline. Quarantine indicates persistent download or decode failures on a specific upstream feed. Investigate the feed's source (Broadcastify URL or stream config) and decide whether to repair, decommission, or mark as expected outage. Quarantine is a hard-stop signal — fires immediately on the first event (no soft buffer).\n\n**Quarantined Feed ID:** $${metric.label.feed_id}\n**Quarantined Feed Name:** $${metric.label.feed_name}\n**Source Type:** $${metric.label.source_type}"
  }

  conditions {
    display_name = "Quarantine event count > 0"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "0s"
      threshold_value = 0

      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.feed_quarantined.name}\"",
        "resource.type=\"gce_instance\"",
      ])

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_DELTA"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["metric.label.feed_id", "metric.label.feed_name", "metric.label.source_type"]
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    auto_close           = "1800s"
    notification_prompts = ["OPENED"]
  }
}

resource "google_monitoring_alert_policy" "fleet_silent" {
  project      = var.project_id
  display_name = "Ingestion fleet silent — no log activity (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "The entire ingestion MIG has emitted zero logs for 10 minutes. This indicates a fleet-wide silent failure (process crash, autoscaler misfire, or networking break preventing log delivery to Cloud Logging) — NOT a partial degradation. Page-and-investigate immediately: SSH to a MIG instance via `gcloud compute ssh` and inspect the ingestion process / journalctl. The 10-minute window is the Cloud Logging metric-ingestion delay floor; going lower causes false pages on transient flushes."
  }

  conditions {
    display_name = "No collector log activity for 10 minutes"

    condition_absent {
      duration = "600s"

      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.collector_log_activity.name}\"",
        "resource.type=\"gce_instance\"",
      ])

      aggregations {
        alignment_period     = "600s"
        per_series_aligner   = "ALIGN_RATE"
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
}

# =============================================================================
# LATENCY ALERTS (ALERT-05a/b/c per-source, ALERT-06 echo Cloud Run)
# =============================================================================

resource "google_monitoring_alert_policy" "chunk_latency_bcfy_feeds" {
  project      = var.project_id
  display_name = "Per-source latency: bcfy_feeds P95 > 45s (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "P95 chunk-processing latency for bcfy_feeds source has exceeded 45 seconds for 5 minutes sustained. bcfy_feeds is the lower-latency source class (live RTSP-style streams) so its threshold is tighter than the 60s polling-source threshold. Investigate VM CPU saturation, GCS upload throughput, or upstream stream interruptions causing buffered re-flushes."
  }

  conditions {
    display_name = "bcfy_feeds P95 chunk latency > 45s sustained 5m"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      threshold_value = 45

      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.chunk_latency.name}\"",
        "resource.type=\"gce_instance\"",
        "metric.labels.source_type=\"bcfy_feeds\"",
      ])

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
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "chunk_latency_openmhz" {
  project      = var.project_id
  display_name = "Per-source latency: openmhz P95 > 60s (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "P95 chunk-processing latency for openmhz source has exceeded 60 seconds for 5 minutes sustained. openmhz is a polling-style source so its threshold is wider than bcfy_feeds. Investigate VM CPU saturation, GCS upload throughput, or upstream openmhz API timeouts/throttling."
  }

  conditions {
    display_name = "openmhz P95 chunk latency > 60s sustained 5m"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      threshold_value = 60

      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.chunk_latency.name}\"",
        "resource.type=\"gce_instance\"",
        "metric.labels.source_type=\"openmhz\"",
      ])

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
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "chunk_latency_bcfy_calls" {
  project      = var.project_id
  display_name = "Per-source latency: bcfy_calls P95 > 60s (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "P95 chunk-processing latency for bcfy_calls source has exceeded 60 seconds for 5 minutes sustained. bcfy_calls is the polling-style call-archive source. Investigate VM CPU saturation, GCS upload throughput, or upstream Broadcastify Calls API timeouts/throttling."
  }

  conditions {
    display_name = "bcfy_calls P95 chunk latency > 60s sustained 5m"

    condition_threshold {
      comparison      = "COMPARISON_GT"
      duration        = "300s"
      threshold_value = 60

      filter = join(" AND ", [
        "metric.type=\"logging.googleapis.com/user/${google_logging_metric.chunk_latency.name}\"",
        "resource.type=\"gce_instance\"",
        "metric.labels.source_type=\"bcfy_calls\"",
      ])

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
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "echo_request_latency" {
  project      = var.project_id
  display_name = "Echo Cloud Run request latency P95 > 60s (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  documentation {
    mime_type = "text/markdown"
    content   = "P95 request latency for the echo Cloud Run service has exceeded 60 seconds for 5 minutes sustained. This is the Cloud Run built-in latency signal (run.googleapis.com/request_latencies) — independent of the log-based chunk_latency metric. Investigate Cloud Run revision health, container cold starts, AlloyDB connection saturation, and Pub/Sub publish backpressure. Distinct signal from the per-source chunk_latency alerts (which measure end-to-end ingestion pipeline timing on the MIG fleet)."
  }

  conditions {
    display_name = "echo Cloud Run P95 request latency > 60s sustained 5m"

    condition_threshold {
      comparison = "COMPARISON_GT"
      duration   = "300s"
      # run.googleapis.com/request_latencies unit = ms; 60000 ms = 60s per ROADMAP criterion 1
      threshold_value = 60000

      filter = join(" AND ", [
        "metric.type=\"run.googleapis.com/request_latencies\"",
        "resource.type=\"cloud_run_revision\"",
        "resource.labels.service_name=\"${local.resolved_echo_service_name}\"",
      ])

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
    auto_close = "1800s"
  }
}

resource "google_logging_metric" "active_feed_count" {
  project     = var.project_id
  name        = "${var.log_metric_name_prefix}_active_feed_count${local.metric_name_suffix}"
  description = "Active feed leases processed by MIG ingestion workers. Extracted from collector_runtime logs."

  filter = <<-EOT
    resource.type="gce_instance"
    AND labels.python_logger="backend.pipeline.ingestion.collector_runtime"
    AND jsonPayload.message =~ "active"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "active_feeds"
      value_type  = "STRING"
      description = "The active feeds count extracted from the log payload."
    }
  }

  label_extractors = {
    "active_feeds" = "REGEXP_EXTRACT(jsonPayload.message, \"— (\\\\d+)/\\\\d+ active\")"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Custom metric: quarantine event signal (emitted by collector runtime)
resource "google_monitoring_metric_descriptor" "quarantine_events" {
  project      = var.project_id
  type         = "custom.googleapis.com/feeds/quarantine_events"
  metric_kind  = "GAUGE"
  value_type   = "INT64"
  description  = "Emitted when a feed transitions to quarantined status."
  display_name = "Feed Quarantine Events"

  labels {
    key         = "feed_id"
    value_type  = "STRING"
    description = "UUID of the quarantined feed."
  }

  labels {
    key         = "feed_name"
    value_type  = "STRING"
    description = "Human-readable name of the feed."
  }

  labels {
    key         = "source_type"
    value_type  = "STRING"
    description = "Feed source type slug (e.g. bcfy_feeds)."
  }
}


