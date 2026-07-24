variable "region" {
  type        = string
  description = "GCP Region"
}

variable "environment" {
  type        = string
  description = "Deployment environment (dev, prod, etc.)"
}

variable "topic_normalized_audio_id" {
  type        = string
  description = "Topic ID for normalized audio."
}

variable "topic_transcribed_audio_id" {
  type        = string
  description = "Topic ID for transcribed audio output."
}

variable "topic_transcribed_audio_dlq_name" {
  type        = string
  description = "DLQ topic name for transcription failures."
}

variable "min_instances" {
  type        = number
  default     = 0
  description = "Minimum number of Cloud Run instances"
}

variable "max_instances" {
  type        = number
  default     = 10
  description = "Maximum number of Cloud Run instances"
}

variable "transcriber_type" {
  type        = string
  default     = "GEMINI"
  description = "Type of transcription model to use."
}

variable "transcriber_config" {
  type        = string
  default     = "{}"
  description = "JSON string of transcriber-specific configuration."
}

variable "audio_segments_service_name" {
  description = "The name of the audio segments service (Cloud Run)."
  type        = string
}

variable "audio_segments_api_url" {
  description = "External API URL handling audio segments."
  type        = string
}

variable "concurrency" {
  type        = number
  default     = 80
  description = "Max instance request concurrency for the transcription service."
}
