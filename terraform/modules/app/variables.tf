variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., prod, pre-prod, dev)."
  type        = string
}



# This is used by the notification service as the endpoint to send alerts to.
# It is provided by TF_VAR_external_endpoint in the deployment.
variable "external_endpoint" {
  type      = string
  sensitive = false
}

# This is used by the notification service as the API key to authenticate with the endpoint.
# It is provided by TF_VAR_external_endpoint_api_key in the deployment.
variable "external_endpoint_api_key" {
  type      = string
  sensitive = true # Prevents the value from appearing in logs
}

variable "ingestion_staging_bucket_name" {
  description = "Name of the GCS bucket for ingestion staging."
  type        = string
}

variable "ingestion_canonical_bucket_name" {
  description = "Name of the GCS bucket for ingestion canonical data."
  type        = string
}

variable "broadcastify_username" {
  description = "Username for Broadcastify/Icecast stream authentication."
  type        = string
  sensitive   = true

  validation {
    condition     = var.broadcastify_username != ""
    error_message = "broadcastify_username must be set to a real value, not 'dummy' or empty."
  }
}

variable "broadcastify_password" {
  description = "Password for Broadcastify/Icecast stream authentication."
  type        = string
  sensitive   = true

  validation {
    condition     = var.broadcastify_password != ""
    error_message = "broadcastify_password must be set to a real value, not 'dummy' or empty."
  }
}

variable "broadcastify_xan_token" {
  description = "XAN token for Broadcastify Icecast relay authentication."
  type        = string
  sensitive   = true
  default     = ""
}

variable "echo_recordings_bucket_name" {
  description = "Name of the GCS bucket for Echo audio recordings."
  type        = string
}

variable "dev_recordings_bucket_name" {
  description = "Dev Echo recordings bucket name. Plumbed to ingestion module; set on prod only (empty in dev) to enable the prod-Echo dual-write mirror into dev."
  type        = string
  default     = ""
}

variable "prod_project_id" {
  description = "Prod GCP project ID. Plumbed to ingestion module; set on dev only (empty in prod) to grant the prod Echo CF cross-project write access on this env's recordings bucket and enable the 7-day TTL."
  type        = string
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

variable "wif_service_account" {
  description = "The service account used by Workload Identity Federation in GitHub Actions. If not provided, it will be constructed based on the environment."
  type        = string
  default     = null
}

variable "feed_change_notifications_enabled" {
  description = "Whether to enable Cloud Logging to Pub/Sub routing for Feed Change Notifications logs."
  type        = bool
  default     = false
}

variable "feed_change_webhook_destination_url" {
  description = "Optional full destination URL for the Feed Change Webhook relay."
  type        = string

  validation {
    condition     = trimspace(var.feed_change_webhook_destination_url) == "" || can(regex("^https?://[^#]+$", var.feed_change_webhook_destination_url))
    error_message = "feed_change_webhook_destination_url must be empty or an absolute HTTP(S) URL."
  }
}

variable "feed_change_webhook_api_key" {
  description = "Optional API key for the Feed Change Webhook relay. Defaults to external_endpoint_api_key."
  type        = string
  sensitive   = true
  default     = ""
}

variable "web_domain" {
  description = "The web domain of the frontend website."
  type        = string
}

variable "google_auth_client_id" {
  description = "Google OAuth Client ID for the frontend."
  type        = string
}

variable "google_auth_client_secret" {
  description = "Google OAuth client secret for frontend authentication."
  type        = string
  sensitive   = true
}

variable "evaluation_min_instances" {
  description = "Minimum instances for evaluation service."
  type        = number
  default     = null
}

variable "evaluation_max_instances" {
  description = "Maximum instances for evaluation service."
  type        = number
  default     = null
}

variable "api_public_url" {
  description = "The public URL of the API (used by Swagger spec). Cannot use the resolved hostname from API Gateway due to circular dependency."
  type        = string
  default     = null
}

variable "workspace_admin_group_email" {
  description = "The email address of the Google Workspace admin group to check for admin status. Can be set to null (e.g. in dev) to allow any authenticated user to act as an admin."
  type        = string
  default     = null
}

variable "enable_monitoring" {
  description = "Forwarded to the ingestion module's monitoring sub-module. Set to true in prod only."
  type        = bool
  default     = false
}

variable "slack_critical_notification_channel_id" {
  description = "Forwarded to the ingestion module. Full GCP resource name of the critical-tier Slack notification channel (format: projects/<id>/notificationChannels/<numeric>). Set in prod by the env composition root; null in dev (no monitoring channel)."
  type        = string
  default     = null
}

variable "transcription_min_instances" {
  description = "Minimum instances for transcription service."
  type        = number
  default     = 0
}

variable "transcription_max_instances" {
  description = "Maximum instances for transcription service."
  type        = number
  default     = 10
}

variable "transcription_concurrency" {
  description = "Max instance request concurrency for transcription service."
  type        = number
  default     = 80
}

variable "transcriber_type" {
  description = "Type of transcription model to use."
  type        = string
  default     = "GOOGLE_CHIRP_V3"
}

variable "transcriber_config" {
  description = "JSON string of transcriber-specific configuration."
  type        = string
  default     = "{}"
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

variable "normalization_max_instance_count" {
  description = "Maximum instances for normalization service."
  type        = number
  default     = 50
}

variable "echo_ingestion_max_instances" {
  description = "Maximum instances for echo audio ingestion service."
  type        = number
  default     = 50
}
