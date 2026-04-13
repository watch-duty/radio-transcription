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
  default     = "n1-standard-2"
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

variable "stop_hour" {
  description = "Hour of the day to stop the VM (0-23), Pacific Time"
  type        = number
  default     = 12
}

variable "start_hour" {
  description = "Hour of the day to start the VM (0-23), Pacific Time"
  type        = number
  default     = 0
}


