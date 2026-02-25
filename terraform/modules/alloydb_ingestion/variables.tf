# -----------------------------------------------------------------------------
# Required Variables
# -----------------------------------------------------------------------------

variable "project_id" {
  description = "The GCP project ID where AlloyDB resources will be created."
  type        = string
}

variable "region" {
  description = "The GCP region for the AlloyDB cluster (e.g. us-central1)."
  type        = string
}

variable "cluster_id" {
  description = "The identifier for the AlloyDB cluster."
  type        = string
}

variable "network_id" {
  description = "The self-link of the VPC network for Private Service Access (e.g. projects/PROJECT/global/networks/NETWORK)."
  type        = string
}

variable "initial_user_password" {
  description = "The password for the initial 'postgres' user. Must be provided by the caller."
  type        = string
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Optional Variables — Database & Users
# -----------------------------------------------------------------------------

variable "worker_user_id" {
  description = "The username for the dedicated worker fleet service account."
  type        = string
  default     = "worker"
}

variable "worker_user_password" {
  description = "The password for the worker fleet user."
  type        = string
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Optional Variables — Cluster
# -----------------------------------------------------------------------------

variable "deletion_protection" {
  description = "Whether to enable deletion protection on the cluster. Set to false for dev/test environments."
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# Optional Variables — Instance
# -----------------------------------------------------------------------------

variable "instance_id" {
  description = "The identifier for the primary instance."
  type        = string
  default     = "primary"
}

variable "machine_cpu_count" {
  description = "The number of vCPUs for the primary instance."
  type        = number
  default     = 2

  validation {
    condition     = var.machine_cpu_count >= 2
    error_message = "AlloyDB requires a minimum of 2 vCPUs."
  }
}

variable "availability_type" {
  description = "Availability type for the primary instance: REGIONAL (HA, multi-zone) or ZONAL (single zone)."
  type        = string
  default     = "REGIONAL"

  validation {
    condition     = contains(["REGIONAL", "ZONAL"], var.availability_type)
    error_message = "availability_type must be REGIONAL or ZONAL."
  }
}

variable "database_flags" {
  description = "A map of database flags to set on the primary instance (e.g. {\"max_connections\" = \"500\"})."
  type        = map(string)
  default     = {}
}

variable "labels" {
  description = "Labels to apply to the cluster and instance resources."
  type        = map(string)
  default     = {}
}

variable "query_insights_enabled" {
  description = "Enable Query Insights on the primary instance for monitoring query performance and lock waits."
  type        = bool
  default     = true
}

variable "query_insights_query_plans_per_minute" {
  description = "Number of query execution plans captured per minute by Query Insights."
  type        = number
  default     = 5
}

# -----------------------------------------------------------------------------
# Optional Variables — Networking
# -----------------------------------------------------------------------------

variable "allocated_ip_range" {
  description = "The name of the allocated IP range for Private Service Access. If null, GCP allocates automatically."
  type        = string
  default     = null
}

# -----------------------------------------------------------------------------
# Optional Variables — Connection Pooling
# -----------------------------------------------------------------------------

variable "connection_pooling_enabled" {
  description = "Enable AlloyDB Managed Connection Pooling on the primary instance. Workers connect on port 6432."
  type        = bool
  default     = true
}

variable "connection_pooling_flags" {
  description = "Flags for Managed Connection Pooling configuration. Common keys: pool_mode (transaction|session), max_pool_size, max_client_connections, query_wait_timeout."
  type        = map(string)
  default = {
    pool_mode = "transaction"
  }
}

# -----------------------------------------------------------------------------
# Optional Variables — Automated Backups
# -----------------------------------------------------------------------------

variable "continuous_backup_enabled" {
  description = "Enable continuous backups for Point-in-Time Recovery (PITR)."
  type        = bool
  default     = true
}

variable "continuous_backup_retention_days" {
  description = "Number of days to retain continuous backups for PITR."
  type        = number
  default     = 14

  validation {
    condition     = var.continuous_backup_retention_days >= 1 && var.continuous_backup_retention_days <= 35
    error_message = "continuous_backup_retention_days must be between 1 and 35."
  }
}

variable "automated_backup_enabled" {
  description = "Enable automated daily backups for the AlloyDB cluster."
  type        = bool
  default     = true
}

variable "backup_retention_count" {
  description = "The number of automated backups to retain."
  type        = number
  default     = 14

  validation {
    condition     = var.backup_retention_count >= 1
    error_message = "backup_retention_count must be at least 1."
  }
}

variable "backup_window" {
  description = "The length of the backup window in protobuf Duration format (e.g. 3600s)."
  type        = string
  default     = "3600s"
}

variable "backup_start_hour" {
  description = "The hour (0-23 UTC) at which automated backups start."
  type        = number
  default     = 2

  validation {
    condition     = var.backup_start_hour >= 0 && var.backup_start_hour <= 23
    error_message = "backup_start_hour must be between 0 and 23."
  }
}
