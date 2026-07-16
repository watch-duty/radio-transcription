variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., prod, pre-prod, dev)."
  type        = string
}

variable "subnet_ip_cidr_range" {
  description = "The IP CIDR range for the primary subnetwork."
  type        = string
  default     = "10.0.0.0/24"
}
