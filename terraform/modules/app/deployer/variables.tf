variable "deployer_sa_email" {
  type        = string
  description = "The service account used by Workload Identity Federation in GitHub Actions to deploy application code."
  default     = null
}

variable "dataflow_staging_bucket_name" {
  type        = string
  description = "Name of the Dataflow staging bucket."
  default     = null
}
