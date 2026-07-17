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

variable "network_id" {
  description = "The ID of the VPC network to attach to."
  type        = string
}

variable "subnet_id" {
  description = "The ID of the subnetwork for resource provisioning."
  type        = string
}

variable "psa_range_name" {
  description = "The name of the Private Service Access allocated IP range."
  type        = string
}

variable "ingestion_staging_bucket_name" {
  description = "The name of the staging GCS bucket."
  type        = string
}

variable "ingestion_canonical_bucket_name" {
  description = "The name of the canonical format GCS bucket."
  type        = string
}

variable "web_domain" {
  description = "The web domain of the frontend website."
  type        = string
}
