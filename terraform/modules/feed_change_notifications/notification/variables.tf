variable "region" {
  description = "The GCP region."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "relay_service_name" {
  description = "The deployed Feed Change Webhook relay Cloud Run service name."
  type        = string
}

variable "relay_service_url" {
  description = "The deployed Feed Change Webhook relay Cloud Run service URL."
  type        = string
}

variable "deployer_service_account_email" {
  description = "Optional Terraform deployer service account email that creates the authenticated Pub/Sub push subscription."
  type        = string
  default     = null
  nullable    = true
}
