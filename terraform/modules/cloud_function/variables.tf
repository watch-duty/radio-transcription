variable "function_name" { type = string }
variable "region" { type = string }
variable "entry_point" { type = string }
variable "source_dir" { type = string }
variable "source_bucket_name" { type = string }

variable "description" {
  type    = string
  default = "Deployed via Terraform"
}

variable "runtime" {
  type    = string
  default = "python312"
}

variable "environment_variables" {
  description = "A map of environment variables to pass to the function"
  type        = map(string)
  default     = {}
}

variable "trigger_topic_id" {
  description = "The full ID of the Pub/Sub topic to trigger this function"
  type        = string
  default     = null
}