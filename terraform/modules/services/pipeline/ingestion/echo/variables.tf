variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "echo_recordings_bucket_name" {
  description = "The S3-compatible bucket name for hardware echo devices."
  type        = string
}

variable "dev_recordings_bucket_name" {
  description = "Name of the dev Echo recordings bucket."
  type        = string
  default     = ""
}

variable "prod_project_id" {
  description = "GCP project ID of the prod environment."
  type        = string
  default     = ""
}

variable "network_name" {
  description = "VPC network name for Cloud Run Direct VPC Egress."
  type        = string
}

variable "subnet_name" {
  description = "VPC subnet name for Cloud Run Direct VPC Egress."
  type        = string
}

variable "alloydb_primary_instance_ip" {
  description = "The primary IP of the AlloyDB instance."
  type        = string
}

variable "alloydb_connection_pooling_port" {
  description = "The pgBouncer pooling port."
  type        = number
}

variable "alloydb_database_name" {
  description = "AlloyDB database name."
  type        = string
}

variable "ingestion_staging_bucket_name" {
  description = "The name of the GCS bucket used for staging audio."
  type        = string
}

variable "topic_segmented_audio_id" {
  description = "Topic ID for segmented audio events."
  type        = string
}

variable "topic_segmented_audio_name" {
  description = "Topic Name for segmented audio events (for IAM)."
  type        = string
}

variable "worker_password_secret_id" {
  description = "Secret Manager secret ID for the AlloyDB worker password."
  type        = string
}

variable "echo_ingestion_max_instances" {
  description = "Maximum instance count for the Echo audio ingestion Cloud Run service."
  type        = number
  default     = 50
}
