variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"
}

variable "wif_service_account" {
  type        = string
  description = "The service account used by Workload Identity Federation in GitHub Actions to deploy application code."
  default     = null
}
