variable "region" {
  description = "The GCP region for the relay Cloud Run service."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string

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
}

variable "deployer_service_account_email" {
  description = "Optional Terraform deployer service account email that creates the Cloud Run service."
  type        = string
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
