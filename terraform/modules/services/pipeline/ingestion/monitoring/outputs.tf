output "log_metric_names" {
  description = "Map from short name to bare log-based metric name (e.g., 'ingestion_chunk_ingested_prod'). Use for debugging; NOT for filter/SLO references (see log_metric_types for those)."
  value = {
    chunk_ingested         = google_logging_metric.chunk_ingested.name
    chunk_latency          = google_logging_metric.chunk_latency.name
    call_download_failed   = google_logging_metric.call_download_failed.name
    feed_quarantined       = google_logging_metric.feed_quarantined.name
    collector_log_activity = google_logging_metric.collector_log_activity.name
    active_feed_count      = google_logging_metric.active_feed_count.name
  }
}

output "log_metric_types" {
  description = "Map from short name to fully-qualified metric type (e.g., 'logging.googleapis.com/user/ingestion_chunk_ingested_prod'). Use in alert and SLO filters."
  value = {
    chunk_ingested         = "logging.googleapis.com/user/${google_logging_metric.chunk_ingested.name}"
    chunk_latency          = "logging.googleapis.com/user/${google_logging_metric.chunk_latency.name}"
    call_download_failed   = "logging.googleapis.com/user/${google_logging_metric.call_download_failed.name}"
    feed_quarantined       = "logging.googleapis.com/user/${google_logging_metric.feed_quarantined.name}"
    collector_log_activity = "logging.googleapis.com/user/${google_logging_metric.collector_log_activity.name}"
    active_feed_count      = "logging.googleapis.com/user/${google_logging_metric.active_feed_count.name}"
  }
}

output "ingestion_service_id" {
  description = "Full service ID of the ingestion custom monitoring service (e.g., 'ingestion-prod'). Use as the `service` field when attaching alert policies or SLOs."
  value       = google_monitoring_custom_service.ingestion.service_id
}

output "echo_service_id" {
  description = "Full service ID of the echo monitoring service (e.g., 'echo-prod'). Use as the `service` field when attaching alert policies or SLOs."
  value       = google_monitoring_service.echo.service_id
}

output "download_slo_name" {
  description = "Full resource name of the download SLO, usable in alert policy filters (format: 'projects/<project>/services/<service>/serviceLevelObjectives/<slo_id>')."
  value       = google_monitoring_slo.download_success.name
}

output "echo_slo_name" {
  description = "Full resource name of the echo SLO, usable in alert policy filters."
  value       = google_monitoring_slo.echo_availability.name
}

output "resolved_echo_service_name" {
  description = "The Cloud Run service name the echo SLO was attached to (e.g., 'echo-audio-ingestion-prod'). Operators should diff this against `gcloud run services list` post-apply — a silent mismatch causes the SLO to match zero data."
  value       = local.resolved_echo_service_name
}
