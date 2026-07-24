variable "project_id" {
  type        = string
  description = "GCP project ID where the monitoring resources are created."
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region for the echo Cloud Run monitoring service selector."
}

variable "environment" {
  type        = string
  default     = "prod"
  description = "Environment suffix appended to every resource ID (e.g., `ingestion-prod`, `download-success-prod`)."

  validation {
    condition     = can(regex("^[a-z0-9]{1,20}$", var.environment))
    error_message = "environment must be lowercase alphanumeric, 1-20 characters. Got: ${var.environment}. Examples: 'prod', 'dev', 'staging'."
  }
}

variable "download_source_types" {
  type        = list(string)
  default     = ["openmhz", "bcfy_calls"]
  description = "Subset of canonical ingestion source types that download audio. Used to restrict the download SLO's good filter. Allowed values: bcfy_feeds, openmhz, bcfy_calls. Echo is deliberately excluded — it has a different monitoring path."

  # The canonical source-type list is inlined because (a) Terraform 1.7.5 (repo
  # pin in .mise.toml) does not allow cross-variable references in validation
  # blocks (added in 1.9+), and (b) no resource in this module consumes the
  # canonical superset — only this subset flows into SLO filters — so declaring
  # a separate `source_types` variable to hold the superset would be dead code
  # that tflint rejects via terraform_unused_declarations. If the allowed set
  # ever changes, update BOTH the default above AND the contains() list below.
  validation {
    condition = alltrue([
      for s in var.download_source_types :
      contains(["bcfy_feeds", "openmhz", "bcfy_calls"], s)
    ])
    error_message = "download_source_types must contain only canonical source types: [bcfy_feeds, openmhz, bcfy_calls]. Got: ${jsonencode(var.download_source_types)}."
  }
}

variable "download_slo_target" {
  type        = number
  default     = 0.90
  description = "Download SLO goal, 28-day rolling. Default 0.90 reflects that external CDNs are less reliable than our own infrastructure."

  validation {
    condition     = var.download_slo_target > 0 && var.download_slo_target < 1
    error_message = "download_slo_target must be in the open interval (0, 1). Got: ${var.download_slo_target}. Typical values: 0.90, 0.95, 0.99."
  }
}

variable "echo_slo_target" {
  type        = number
  default     = 0.995
  description = "Echo SLO goal, 28-day rolling. Default 0.995 reflects Cloud Run's expected availability."

  validation {
    condition     = var.echo_slo_target > 0 && var.echo_slo_target < 1
    error_message = "echo_slo_target must be in the open interval (0, 1). Got: ${var.echo_slo_target}. Typical values: 0.99, 0.995, 0.999."
  }
}

variable "echo_service_name" {
  type        = string
  default     = ""
  description = "Cloud Run service name for the echo SLO's basic service selector. Empty string means the default `echo-audio-ingestion-$${var.environment}` will be used (see main.tf locals)."
}

variable "log_metric_name_prefix" {
  type        = string
  default     = "ingestion"
  description = "Prefix for every log-based metric name (e.g., `ingestion_chunk_ingested_prod`)."
}

variable "notification_channel_id" {
  description = "GCP notification channel resource id forwarded from the parent ingestion module. Generic name (not Slack-specific) so future channel types can flow through. Consumed by the alert policies in main.tf via `notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []`. Default null preserves dev behavior — though the monitoring sub-module is also gated by var.enable_monitoring (false in dev), so this null-fallback is defense-in-depth rather than load-bearing."
  type        = string
  default     = null
}
