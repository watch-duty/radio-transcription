variable "region" {
  type        = string
  description = "GCP Region"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"
}

variable "canonical_audio_bucket_name" {
  type        = string
  description = "The GCS bucket name where canonical audio files are stored"
}

variable "staging_audio_bucket_name" {
  type        = string
  description = "The GCS bucket name where staging/raw audio files are stored"
}

variable "topic_normalized_audio_id" {
  type        = string
  description = "The ID of the normalized_audio Pub/Sub topic"
}

variable "topic_segmented_audio_id" {
  type        = string
  description = "The ID of the segmented_audio Pub/Sub topic"
}

variable "audio_segments_api_url" {
  type        = string
  description = "The URL of the Audio Segments API service"
  default     = null
}

variable "audio_segments_service_name" {
  type        = string
  description = "The service name of the Audio Segments API (for IAM role binding)"
}

variable "topic_normalized_audio_dlq_name" {
  type        = string
  description = "The name of the normalized audio dead letter queue topic"
}

variable "max_instance_count" {
  type        = number
  description = "The maximum number of instances for the normalization service"
  default     = 50
}
