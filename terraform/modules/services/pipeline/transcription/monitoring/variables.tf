variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"
}

variable "notification_channel_id" {
  type        = string
  default     = null
  description = "GCP notification channel resource ID for alerts."
}

