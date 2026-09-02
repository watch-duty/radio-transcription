variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "network_id" {
  description = "The VPC network ID for allowing IAP SSH."
  type        = string
}

variable "subnet_id" {
  description = "The subnet ID for VPC access."
  type        = string
}

variable "broadcastify_username" {
  description = "The Broadcastify username for the ingestion collector."
  type        = string
  sensitive   = true
}

variable "broadcastify_password" {
  description = "The Broadcastify password for the ingestion collector."
  type        = string
  sensitive   = true
}

variable "broadcastify_xan_token" {
  description = "XAN token for Broadcastify Icecast relay authentication."
  type        = string
  sensitive   = true
  default     = ""
}

variable "broadcastify_api_key" {
  description = "API key for Broadcastify credential rotation."
  type        = string
  sensitive   = true
}

variable "broadcastify_api_key_id" {
  description = "API key ID for Broadcastify credential rotation."
  type        = string
  sensitive   = true
}

variable "broadcastify_api_app_id" {
  description = "API app ID for Broadcastify credential rotation."
  type        = string
  sensitive   = true
}

variable "alloydb_primary_instance_ip" {
  description = "The primary IP of the AlloyDB instance."
  type        = string
}

variable "alloydb_worker_password" {
  description = "The master password for the worker role in AlloyDB."
  type        = string
  sensitive   = true
}

variable "alloydb_connection_pooling_port" {
  description = "The pgBouncer pooling port."
  type        = number
}

variable "alloydb_database_name" {
  description = "AlloyDB database name."
  type        = string
}

variable "ingestion_staging_bucket_name" {
  description = "The name of the GCS bucket used for staging audio."
  type        = string
}

variable "topic_continuous_audio_id" {
  description = "Topic ID for continuous audio events."
  type        = string
}

variable "topic_continuous_audio_name" {
  description = "Topic Name for continuous audio events (for IAM)."
  type        = string
}

variable "topic_segmented_audio_id" {
  description = "Topic ID for segmented audio events."
  type        = string
}

variable "topic_segmented_audio_name" {
  description = "Topic Name for segmented audio events (for IAM)."
  type        = string
}

variable "echo_recordings_bucket_name" {
  description = "The S3-compatible bucket name for hardware echo devices."
  type        = string
}

variable "fire_notifications_url_base" {
  description = "The base URL for fire notifications audio."
  type        = string
  default     = "https://audioplay.textmefires.info/api/audio/"
}

variable "fire_notifications_s3_base" {
  description = "The base S3 path for fire notifications audio."
  type        = string
  default     = "https://s3.amazonaws.com/fn-sdr-test1/new-nas/"
}


# -----------------------------------------------------------------------------
# Echo dev-mirror dual-write (cross-project)
# -----------------------------------------------------------------------------
# When set in PROD, the Echo CF copies each ingested MP3 into the dev
# recordings bucket so dev's pipeline runs E2E against real prod traffic.
# DEV's `feeds` table acts as the channel allowlist (CF short-circuits on
# unmatched channels). Both variables default to "" so a single-env apply
# (only prod, or only dev) is a no-op for the other side.

variable "dev_recordings_bucket_name" {
  description = "Name of the dev Echo recordings bucket. Set on prod only; injects DEV_RECORDINGS_BUCKET env on the Echo CF for the dual-write mirror. Empty string disables the mirror."
  type        = string
  default     = ""
}

variable "prod_project_id" {
  description = "GCP project ID of the prod environment. Set on dev only; grants the prod Echo CF's runtime SA storage.objectCreator on the dev recordings bucket. Empty string disables the cross-project IAM grant."
  type        = string
  default     = ""
}

variable "network_name" {
  description = "VPC network name for Cloud Run Direct VPC Egress."
  type        = string
}

variable "subnet_name" {
  description = "VPC subnet name for Cloud Run Direct VPC Egress."
  type        = string
}


variable "worker_password_secret_id" {
  description = "Secret Manager secret ID for the AlloyDB worker password."
  type        = string
}

variable "enable_monitoring" {
  description = "When true, instantiates the nested monitoring sub-module (log metrics, SLOs). Prod-only for now; dev leaves default false."
  type        = bool
  default     = false
}

variable "slack_critical_notification_channel_id" {
  description = "Slack notification channel id forwarded to the broadcastify_credential_rotation and monitoring sub-modules, where it lands as `var.notification_channel_id`. Set in prod by terraform/environments/prod/main.tf (= google_monitoring_notification_channel.slack_critical.id); null in dev (no Slack channel exists there). Alert policies in both sub-modules use `notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []` so they remain visible in dev's GCP Console for debugging while routing only to Slack in prod."
  type        = string
  default     = null
}

variable "fire_notifications_user" {
  description = "The username for fire notifications audio."
  type        = string
  sensitive   = true
}

variable "fire_notifications_password" {
  description = "The password for fire notifications audio."
  type        = string
  sensitive   = true
}

variable "cos_image_self_link" {
  type        = string
  description = "COS Image self_link"
}

variable "available_zones" {
  type        = list(string)
  description = "Available zones in the region"
}

variable "echo_ingestion_max_instances" {
  description = "Maximum instance count for the Echo audio ingestion Cloud Run service."
  type        = number
  default     = 50
}

variable "collector_machine_type" {
  description = "Compute Engine machine type for ingestion collector VMs (e.g. n2-standard-2, n2-standard-4)."
  type        = string
  default     = "n2-standard-2"
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "project_number" {
  description = "The GCP project number."
  type        = string
}
