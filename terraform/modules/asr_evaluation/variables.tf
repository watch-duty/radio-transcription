variable "name" {
  description = "Name of the GCE instance"
  type        = string
}

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "machine_type" {
  description = "The machine type to use"
  type        = string
  default     = "n1-standard-4"
}

variable "gpu_type" {
  description = "The type of GPU to attach"
  type        = string
  default     = "nvidia-tesla-t4"
}

variable "gpu_count" {
  description = "Number of GPUs to attach"
  type        = number
  default     = 1
}

variable "zone" {
  description = "The zone to spawn the instance in"
  type        = string
  default     = "us-central1-a"
}

variable "auto_shutdown_hours" {
  description = "Number of hours to run before auto-shutdown threshold is reached"
  type        = number
  default     = 4
}


