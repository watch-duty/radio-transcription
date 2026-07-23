variable "relay_service_name" {
  description = "The deployed Feed Change Webhook relay Cloud Run service name."
  type        = string
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
}

variable "push_subscription_name" {
  description = "Name of the main Pub/Sub push subscription to monitor."
  type        = string
}

variable "dlq_subscription_name" {
  description = "Name of the DLQ Pub/Sub subscription to monitor."
  type        = string
}

variable "slack_critical_notification_channel_id" {
  description = "Optional Slack notification channel for alerts."
  type        = string
  default     = null
}
