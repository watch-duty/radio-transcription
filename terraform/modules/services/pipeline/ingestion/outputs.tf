output "collector_mig_id" {
  description = "The instance group manager ID for the ingestion collector MIG."
  value       = module.collector_mig.instance_group_manager_id
}

output "collector_instance_group" {
  description = "The instance group self-link for the ingestion collector."
  value       = module.collector_mig.instance_group_self_link
}

output "echo_recordings_uploader_hmac_access_id" {
  description = "The HMAC access ID for uploading Echo recordings."
  value       = google_storage_hmac_key.echo_recordings_uploader.access_id
}

output "echo_recordings_uploader_hmac_secret" {
  description = "The HMAC secret for uploading Echo recordings."
  value       = google_storage_hmac_key.echo_recordings_uploader.secret
  sensitive   = true
}

output "monitoring_log_metric_names" {
  description = "Bare log-based metric names from the monitoring sub-module (null when enable_monitoring=false). Map keyed by short name (chunk_ingested, chunk_latency, call_download_failed, feed_quarantined, collector_log_activity)."
  value       = try(module.monitoring[0].log_metric_names, null)
}

output "monitoring_log_metric_types" {
  description = "Fully-qualified log-based metric types (e.g., 'logging.googleapis.com/user/ingestion_chunk_ingested_prod') from the monitoring sub-module (null when disabled). Use in alert/SLO filters."
  value       = try(module.monitoring[0].log_metric_types, null)
}

output "monitoring_ingestion_service_id" {
  description = "Custom monitoring service ID for ingestion (e.g., 'ingestion-prod'); null when disabled."
  value       = try(module.monitoring[0].ingestion_service_id, null)
}

output "monitoring_echo_service_id" {
  description = "Monitoring service ID for echo (e.g., 'echo-prod'); null when disabled."
  value       = try(module.monitoring[0].echo_service_id, null)
}

output "monitoring_download_slo_name" {
  description = "Full resource name of the download SLO (null when disabled)."
  value       = try(module.monitoring[0].download_slo_name, null)
}

output "monitoring_echo_slo_name" {
  description = "Full resource name of the echo SLO (null when disabled)."
  value       = try(module.monitoring[0].echo_slo_name, null)
}

output "monitoring_resolved_echo_service_name" {
  description = "Cloud Run service name the echo SLO attached to (null when disabled). Operators should diff this against `gcloud run services list` post-apply — silent mismatch causes zero data."
  value       = try(module.monitoring[0].resolved_echo_service_name, null)
}

output "echo_service_name" {
  description = "The name of the deployed echo ingestion Cloud Run service."
  value       = google_cloud_run_v2_service.echo_ingestion.name
}
