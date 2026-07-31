variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"
}

variable "notification_channel_id" {
  type        = string
  description = "Cloud Monitoring notification channel ID."
  default     = null
}
