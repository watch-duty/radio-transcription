variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., prod, pre-prod, dev)."
  type        = string
}

variable "network_name" {
  description = "The VPC network name for VPC access."
  type        = string
}

variable "subnet_name" {
  description = "The subnet name for VPC access."
  type        = string
}

variable "alloydb_instance_ip" {
  description = "The private IP of the AlloyDB primary instance."
  type        = string
}

variable "alloydb_connection_pooling_port" {
  description = "The connection pooling port."
  type        = number
}

variable "worker_password_secret_id" {
  description = "The Secret Manager secret ID for the worker password."
  type        = string
}

variable "radio_transcription_db_name" {
  description = "The name of the database"
  type        = string
}
