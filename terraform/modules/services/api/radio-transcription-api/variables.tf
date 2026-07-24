variable "region" {
  description = "The GCP region for all resources."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "allowed_origin" {
  description = "The origin that is allowed to make requests to the API Gateway."
  type        = string
}

variable "rules_api_url" {
  description = "The URL of the rules management API."
  type        = string
}

variable "feeds_api_url" {
  description = "The URL of the feed store API."
  type        = string
}

variable "audio_segments_api_url" {
  description = "The URL of the audio segments API."
  type        = string
}

variable "google_auth_client_id" {
  description = "The Google OAuth client ID used for authenticating users."
  type        = string
}

variable "google_auth_client_secret" {
  description = "Google OAuth client secret for frontend authentication."
  type        = string
  sensitive   = true
}

variable "api_public_url" {
  description = "The public URL of the API (used by Swagger spec). Cannot use the resolved hostname from API Gateway due to circular dependency."
  type        = string
  default     = null
}

variable "workspace_admin_group_email" {
  description = "The email address of the Google Workspace admin group to check for admin status. Can be set to null (e.g. in dev) to allow any authenticated user to act as an admin."
  type        = string
  default     = null
}
