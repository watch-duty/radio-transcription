variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "rules_api_url" {
  description = "External API URL handling evaluation."
  type        = string
}

variable "topic_evaluated_audio_id" {
  description = "Topic ID for evaluated audio results (destination)."
  type        = string
}

variable "topic_transcribed_audio_id" {
  description = "Topic ID for transcribed audio (trigger source)."
  type        = string
}

variable "rules_service_name" {
  description = "The name of the rules service (Cloud Run)."
  type        = string
}

variable "audio_segments_service_name" {
  description = "The name of the audio segments service (Cloud Run)."
  type        = string
}

variable "audio_segments_api_url" {
  description = "External API URL handling audio segments."
  type        = string
}

variable "topic_evaluated_audio_dead_letter_name" {
  description = "The dead letter topic name for evaluation."
  type        = string
}

variable "min_instances" {
  description = "The minimum number of instances to keep warm."
  type        = number
  default     = null
}

variable "max_instances" {
  description = "The maximum number of instances to scale up to."
  type        = number
  default     = null
}
