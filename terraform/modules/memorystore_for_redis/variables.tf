# -----------------------------------------------------------------------------
# Required
# -----------------------------------------------------------------------------

variable "project_id" {
  description = "The GCP project ID where the database will be created."
  type        = string
}

variable "name" {
  description = "The globally unique name of the Memorystore for Redis database."
  type        = string
}

variable "display_name" {
  description = "Display name of the instance."
  type        = string
}

variable "region" {
  description = "The region this instance will be deployed to."
  type        = string
}

variable "memory_size_gb" {
  description = "Redis memory size in GiB."
  type        = number

  validation {
    condition     = var.read_replicas_mode == "READ_REPLICAS_DISABLED" || var.memory_size_gb >= 5
    error_message = "Minimum capacity is 5 GB when read replicas are enabled"
  }
}

variable "network_id" {
  description = "The ID of the VPC network for Private Service Access."
  type        = string
}

# -----------------------------------------------------------------------------
# Optional
# -----------------------------------------------------------------------------

variable "tier" {
  description = "The service tier to use for for the architecture of Memorystore for Redis. Note that it is not possible to upgrade from one tier to another."
  type        = string
  default     = "BASIC"

  validation {
    condition     = contains(["BASIC", "STANDARD_HA"], var.tier)
    error_message = "tier must be one of: BASIC, STANDARD_HA."
  }
}

variable "connect_mode" {
  description = "The connection mode of the Redis instance."
  type        = string
  default     = "DIRECT_PEERING"

  validation {
    condition     = contains(["DIRECT_PEERING", "PRIVATE_SERVICE_ACCESS"], var.connect_mode)
    error_message = "connect_mode must be one of: DIRECT_PEERING, PRIVATE_SERVICE_ACCESS."
  }
}

variable "read_replicas_mode" {
  description = "Read replica mode. Can only be specified when trying to create the instance. Not applicable for BASIC tier."
  type        = string
  default     = "READ_REPLICAS_DISABLED"
}

variable "replica_count" {
  description = "The number of read replicas."
  type        = number
  default     = 0

  validation {
    # The valid range for the Standard Tier with read replicas enabled is [1-5] and defaults to 2. 
    # If read replicas are not enabled for a Standard Tier instance, the only valid value is 1 and the default is 1.
    # The valid value for basic tier is 0 and the default is also 0.
    condition     = var.tier == "BASIC" ? var.replica_count == 0 : var.read_replicas_mode == "READ_REPLICAS_DISABLED" ? var.replica_count == 1 : var.replica_count >= 1 && var.replica_count <= 5
    error_message = "Invalid value for replica_count for tier/replica mode."
  }
}

variable "auth_enabled" {
  description = "Indicates whether OSS Redis AUTH is enabled for the instance."
  type        = bool
  default     = false
}

variable "transit_encryption_mode" {
  description = "TLS mode of the Redis instance. Recommended to enable this if auth_enabled is also enabled to encrypt the credentials."
  type        = string
  default     = "DISABLED"

  validation {
    condition     = contains(["DISABLED", "SERVER_AUTHENTICATION"], var.transit_encryption_mode)
    error_message = "transit_encryption_mode must be one of: DISABLED, SERVER_AUTHENTICATION."
  }
}
