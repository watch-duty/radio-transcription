variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "network_name" {
  description = "The VPC network name for VPC access."
  type        = string
}

variable "subnet_name" {
  description = "The subnet name for VPC access."
  type        = string
}

variable "external_endpoint" {
  description = "The endpoint URL for the external backend."
  type        = string
}

variable "external_endpoint_api_key" {
  description = "The API key for the external backend endpoint."
  type        = string
  sensitive   = true
}

variable "redis_host" {
  description = "The IP address or hostname of the Redis instance."
  type        = string
}

variable "redis_port" {
  description = "The port of the Redis instance."
  type        = number
}

variable "redis_password_secret_id" {
  description = "The Secret Manager secret ID for the Redis password."
  type        = string
}

variable "redis_password_secret_full_id" {
  description = "The full Secret Manager resource ID for the Redis password."
  type        = string
}

variable "redis_certificate_secret_id" {
  description = "The Secret Manager secret ID for the Redis certificate."
  type        = string
}

variable "redis_certificate_secret_full_id" {
  description = "The full Secret Manager resource ID for the Redis certificate."
  type        = string
}

variable "topic_evaluated_audio_id" {
  description = "Topic ID for evaluated audio results (trigger source)."
  type        = string
}

variable "topic_evaluated_audio_dead_letter_id" {
  description = "Topic ID for evaluated audio results dead letter queue."
  type        = string
}

variable "webapp_base_url" {
  description = "The base URL of the webapp. Must be a fully qualified HTTPS URL (for example, https://example.com) and must not include a trailing slash."
  type        = string

  validation {
    condition     = can(regex("^https://[^/]+(?:/[^/].*)?$", var.webapp_base_url)) && !endswith(var.webapp_base_url, "/")
    error_message = "webapp_base_url must be a fully qualified HTTPS URL with a scheme (for example, https://example.com) and must not end with a trailing slash."
  }
}

variable "feeds_api_url" {
  description = "The URL of the feeds API service."
  type        = string
}

variable "rules_api_url" {
  description = "Base URL of the rules management API service; used to attach triggered rules' tags to notifications. The client appends /v1/rules."
  type        = string
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "project_number" {
  description = "The GCP project number."
  type        = string
}

