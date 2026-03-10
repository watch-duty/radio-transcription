# -----------------------------------------------------------------------------
# Required Variables
# -----------------------------------------------------------------------------

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region."
  type        = string
}

variable "zone" {
  description = "The GCE zone for the zonal MIG (e.g. us-central1-a)."
  type        = string
}

variable "name_prefix" {
  description = "Name prefix for all resources (instance template, MIG, systemd service). Must be 1-48 characters."
  type        = string

  validation {
    condition     = can(regex("^[a-z][-a-z0-9]{0,47}$", var.name_prefix))
    error_message = "name_prefix must be 1-48 chars, start with a lowercase letter, and contain only lowercase letters, digits, and hyphens."
  }
}

variable "subnetwork_id" {
  description = "The self-link of the subnet where instances are placed."
  type        = string
}

variable "service_account_email" {
  description = "The service account email for the GCE instances."
  type        = string
}

variable "container_image" {
  description = "Full Artifact Registry image path (e.g. us-central1-docker.pkg.dev/project/repo/image:tag)."
  type        = string
}

# -----------------------------------------------------------------------------
# Optional Variables
# -----------------------------------------------------------------------------

variable "container_env" {
  description = "Environment variables passed to the container via docker run -e flags."
  type        = map(string)
  default     = {}
  sensitive   = true
}

variable "machine_type" {
  description = "The GCE machine type for each instance."
  type        = string
  default     = "e2-small"
}

variable "target_size" {
  description = "Fixed number of instances in the MIG."
  type        = number
  default     = 1

  validation {
    condition     = var.target_size >= 0
    error_message = "target_size must be non-negative."
  }
}

variable "boot_disk_size_gb" {
  description = "Boot disk size in GB."
  type        = number
  default     = 20
}

variable "labels" {
  description = "Labels to apply to GCE resources."
  type        = map(string)
  default     = {}
}
