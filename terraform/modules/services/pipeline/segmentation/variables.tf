variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"
}

variable "wif_service_account" {
  type        = string
  description = "The service account used by Workload Identity Federation in GitHub Actions to deploy application code."
  default     = null
}

variable "enable_monitoring" {
  type        = bool
  description = "Whether to enable Cloud Monitoring log-based metrics, SLOs, and alert policies."
  default     = true
}

variable "notification_channel_id" {
  type        = string
  description = "Cloud Monitoring notification channel ID for critical alerts."
  default     = null
}

