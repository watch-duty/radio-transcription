variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., prod, dev)."
  type        = string
}

variable "network_name" {
  description = "VPC network name for Cloud Run Direct VPC Egress (PRIVATE_RANGES_ONLY)."
  type        = string
}

variable "subnet_name" {
  description = "VPC subnet name for Cloud Run Direct VPC Egress."
  type        = string
}

variable "alloydb_primary_instance_ip" {
  description = "AlloyDB primary instance private IP. Publisher reaches it through PgBouncer transaction-mode pooling."
  type        = string
}

variable "alloydb_connection_pooling_port" {
  description = "PgBouncer pooling port (typically 6432). Publisher's asyncpg connect uses this."
  type        = number
}

variable "alloydb_database_name" {
  description = "AlloyDB database name."
  type        = string
}

variable "worker_password_secret_id" {
  description = "Secret Manager secret ID for the AlloyDB worker password. Publisher injects via secret_key_ref env var (not runtime SDK access). Reused — already plumbed from module.storage in app/main.tf; not a new secret."
  type        = string
}

variable "notification_channel_id" {
  description = "Cloud Monitoring notification channel ID forwarded from the parent ingestion module's slack_critical_notification_channel_id. Alert policies use `notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []` so they remain visible in dev's GCP Console for debugging while routing only to Slack in prod (where the channel is wired)."
  type        = string
  default     = null
}
