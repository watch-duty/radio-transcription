output "alloydb_cluster_name" {
  description = "The AlloyDB cluster name."
  value       = module.storage.alloydb_cluster_name
}

output "alloydb_instance_ip" {
  description = "The private IP of the AlloyDB primary instance."
  value       = module.storage.alloydb_primary_instance_ip
  sensitive   = true
}

output "alloydb_connection_pooling_port" {
  description = "The connection pooling port."
  value       = module.storage.alloydb_connection_pooling_port
}

output "postgres_password_secret_id" {
  description = "The Secret Manager secret ID for the postgres password."
  value       = module.storage.postgres_password_secret_id
}

output "worker_password_secret_id" {
  description = "The Secret Manager secret ID for the worker password."
  value       = module.storage.worker_password_secret_id
}

output "evaluation_service_url" {
  description = "The URL where the Evaluation Pipeline is hosted"
  value       = module.evaluation.evaluation_service_uri
}

output "rules_api_url" {
  description = "The URL where the Rules API Service is hosted"
  value       = module.rules_api.rules_api_url
}

output "rules_api_service_name" {
  description = "The exact name of the Rules API Service"
  value       = module.rules_api.rules_api_service_name
}

output "evaluation_service_name" {
  description = "The exact name of the service (used by GitHub Actions)"
  value       = module.evaluation.evaluation_service_name
}

output "topic_transcribed_audio" {
  description = "The Pub/Sub topic that triggers the pipeline"
  value       = module.message_queues.topic_transcribed_audio_id
}

output "topic_evaluated_audio" {
  description = "The Pub/Sub topic where results are published"
  value       = module.message_queues.topic_evaluated_audio_id
}

output "topic_continuous_audio" {
  description = "The Pub/Sub topic for continuous audio available (GCS paths of audio to transcribe)."
  value       = module.message_queues.topic_continuous_audio_id
}

output "ingestion_staging_bucket_name" {
  description = "The name of the staging GCS bucket."
  value       = module.storage.ingestion_staging_bucket_name
}

output "ingestion_canonical_bucket_name" {
  description = "The name of the canonical GCS bucket."
  value       = module.storage.ingestion_canonical_bucket_name
}

output "collector_mig_id" {
  description = "The instance group manager ID for the ingestion collector MIG."
  value       = module.ingestion.collector_mig_id
}

output "collector_instance_group" {
  description = "The instance group self-link for the ingestion collector."
  value       = module.ingestion.collector_instance_group
}

output "echo_recordings_uploader_hmac_access_id" {
  description = "HMAC access ID for the Echo recordings uploader."
  value       = module.ingestion.echo_recordings_uploader_hmac_access_id
}

output "echo_recordings_uploader_hmac_secret" {
  description = "HMAC secret for the Echo recordings uploader."
  value       = module.ingestion.echo_recordings_uploader_hmac_secret
  sensitive   = true
}

output "radio_transcription_api_url" {
  description = "The URL of the radio transcription API"
  value       = module.radio_transcription_api.radio_transcription_api_url
}

output "monitoring_log_metric_names" {
  description = "Log-based metric names from the monitoring sub-module (null when enable_monitoring=false)."
  value       = module.ingestion.monitoring_log_metric_names
}

output "monitoring_log_metric_types" {
  description = "Fully-qualified log-based metric types (null when disabled)."
  value       = module.ingestion.monitoring_log_metric_types
}

output "monitoring_ingestion_service_id" {
  description = "Custom monitoring service ID for ingestion (null when disabled)."
  value       = module.ingestion.monitoring_ingestion_service_id
}

output "monitoring_echo_service_id" {
  description = "Monitoring service ID for echo (null when disabled)."
  value       = module.ingestion.monitoring_echo_service_id
}

output "monitoring_download_slo_name" {
  description = "Download SLO resource name (null when disabled)."
  value       = module.ingestion.monitoring_download_slo_name
}

output "monitoring_echo_slo_name" {
  description = "Echo SLO resource name (null when disabled)."
  value       = module.ingestion.monitoring_echo_slo_name
}

output "monitoring_resolved_echo_service_name" {
  description = "Cloud Run service name the echo SLO bound to (null when disabled)."
  value       = module.ingestion.monitoring_resolved_echo_service_name
}
