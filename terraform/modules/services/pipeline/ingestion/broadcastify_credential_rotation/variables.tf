variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., prod, staging)."
  type        = string
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

variable "broadcastify_username" {
  description = "Broadcastify username used by the rotation function to authenticate and obtain new tokens."
  type        = string
  sensitive   = true
}

variable "broadcastify_password" {
  description = "Broadcastify password used by the rotation function to authenticate and obtain new tokens."
  type        = string
  sensitive   = true
}

variable "notification_channel_id" {
  description = "GCP notification channel resource id forwarded from the parent ingestion module. Generic name (not Slack-specific) so future channel types can flow through the same plumbing. Consumed by the broadcastify alert policies via `notification_channels = var.notification_channel_id != null ? [var.notification_channel_id] : []`. Default null in dev keeps the alert policies present (visible in GCP Console for debugging) but routes to no channel — alerts fire silently rather than paging."
  type        = string
  default     = null
}

variable "project_number" {
  type        = string
  description = "GCP Project Number"
}

