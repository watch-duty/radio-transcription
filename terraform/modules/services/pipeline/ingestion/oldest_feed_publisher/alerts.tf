# =============================================================================
# OLDEST-FEED PUBLISHER — STALENESS ALERT
# =============================================================================
# Publisher emits custom.googleapis.com/feeds/unclaimed_count every 60s
# (Cloud Scheduler tick). MIG autoscaler scales on this metric. If publish
# stops (silent hang, scheduler down, function crash, DB outage), autoscaler
# holds the last value indefinitely.
#
# This single condition_absent alert catches every failure mode that stops
# the metric from arriving. 180s threshold = 3 missed ticks; tight enough to
# bound autoscaler stale data, loose enough to absorb one ingestion-delay
# spike. Custom metric write→read latency is typically <60s.
#
# Aggregation choice (ALIGN_MEAN/REDUCE_MEAN) is the standard idiom for INT64
# GAUGE metrics under condition_absent. Do NOT copy the ALIGN_RATE/REDUCE_SUM
# pattern from monitoring/main.tf's fleet_silent — that alert operates on a
# log-based DELTA metric (counter of log events), which is a different metric
# kind. ALIGN_RATE on a gauge has undefined semantics (rate-of-change between
# snapshots, ~0 for a stable count) and could cause false negatives.
#
# notification_channels gating mirrors the pattern used in
# broadcastify_credential_rotation/ and monitoring/ — null in dev, wired to
# slack_critical in prod, alert remains visible in the GCP Console either way.

resource "google_monitoring_alert_policy" "publisher_metric_absent" {
  project      = var.project_id
  display_name = "oldest-feed-publisher metric absent (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []

  conditions {
    display_name = "No unclaimed_count datapoint for 180 seconds"

    condition_absent {
      duration = "180s"
      filter = join(" AND ", [
        "metric.type=\"custom.googleapis.com/feeds/unclaimed_count\"",
        "resource.type=\"global\"",
      ])

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_MEAN"
      }

      trigger {
        count = 1
      }
    }
  }

  alert_strategy {
    # 7 days. condition_absent on a metric that may STOP being written
    # (publisher down) needs a long auto_close: when no datapoints arrive,
    # Cloud Monitoring eventually GCs the time series, and a short auto_close
    # would silently resolve the incident even though the outage persists.
    # 7 days keeps the incident open for the duration of any realistic
    # sustained outage so on-call sees it until manually acknowledged.
    auto_close = "604800s"
  }

  user_labels = {
    environment = var.environment
    service     = "radio-transcription"
    component   = "ingestion-publisher"
    tier        = "critical"
  }

  documentation {
    content   = <<-EOT
      The oldest-feed-publisher Cloud Run service has not emitted a
      `custom.googleapis.com/feeds/unclaimed_count` datapoint in the
      last 180s. The MIG autoscaler is currently scaling on the last
      published value.

      Triage:
      1. `gcloud scheduler jobs describe oldest-feed-publisher-${var.environment} --location=${var.region}`
      2. `gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="oldest-feed-publisher-${var.environment}"' --limit=20 --freshness=30m`
      3. AlloyDB connectivity from the publisher's VPC subnet.
    EOT
    mime_type = "text/markdown"
  }
}
