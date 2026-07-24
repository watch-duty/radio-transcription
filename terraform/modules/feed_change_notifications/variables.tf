variable "region" {
  type        = string
  description = "GCP Region"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"

  validation {
    condition     = can(regex("^[a-z0-9]([a-z0-9-]*[a-z0-9])?$", var.environment))
    error_message = "environment must contain only lowercase letters, numbers, and hyphens, and must not start or end with a hyphen."
  }
}

variable "webhook_url" {
  description = "Destination webhook URL. Must include the HTTP(S) scheme."
  type        = string

  validation {
    condition     = can(regex("^https?://[^#]+$", var.webhook_url))
    error_message = "webhook_url must be an absolute HTTP(S) URL."
  }
}

variable "webhook_api_key" {
  description = "API key sent to the destination webhook as X-Api-Key."
  type        = string
  sensitive   = true

  validation {
    condition     = trimspace(var.webhook_api_key) != ""
    error_message = "webhook_api_key must not be empty."
  }
}

variable "deployer_service_account_email" {
  type        = string
  description = "Optional Terraform deployer service account email that creates the authenticated Pub/Sub push subscription."
  default     = null
  nullable    = true

  validation {
    condition = (
      var.deployer_service_account_email == null
      || trimspace(var.deployer_service_account_email) == ""
      || (
        !can(regex("[[:space:]]", var.deployer_service_account_email))
        && can(regex("^[^:@]+@[^@]+$", var.deployer_service_account_email))
      )
    )
    error_message = "deployer_service_account_email must be null, empty, or a bare service-account email without whitespace."
  }
}

variable "slack_critical_notification_channel_id" {
  description = "Optional Slack notification channel for Feed Change Notifications delivery health alerts."
  type        = string
  default     = null
}
