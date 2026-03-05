# -----------------------------------------------------------------------------
# Required
# -----------------------------------------------------------------------------

variable "project_id" {
  description = "The GCP project ID where the bucket will be created."
  type        = string
}

variable "name" {
  description = "The globally unique name of the GCS bucket."
  type        = string
}

# -----------------------------------------------------------------------------
# Optional
# -----------------------------------------------------------------------------

variable "location" {
  description = "The GCS location (region or multi-region) for the bucket."
  type        = string
  default     = "US"
}

variable "storage_class" {
  description = "The storage class of the bucket (STANDARD, NEARLINE, COLDLINE, ARCHIVE)."
  type        = string
  default     = "STANDARD"

  validation {
    condition     = contains(["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"], var.storage_class)
    error_message = "storage_class must be one of: STANDARD, NEARLINE, COLDLINE, ARCHIVE."
  }
}

variable "force_destroy" {
  description = "If true, all objects in the bucket will be deleted when the bucket is destroyed via Terraform."
  type        = bool
  default     = false
}

variable "labels" {
  description = "Labels to apply to the bucket."
  type        = map(string)
  default     = {}
}

variable "lifecycle_rules" {
  description = "A list of lifecycle rules to apply to the bucket. Each rule requires an action and a condition."
  type = list(object({
    action = object({
      type          = string
      storage_class = optional(string)
    })
    condition = object({
      age                   = optional(number)
      created_before        = optional(string)
      with_state            = optional(string)
      num_newer_versions    = optional(number)
      matches_storage_class = optional(list(string))
    })
  }))
  default = []
}
