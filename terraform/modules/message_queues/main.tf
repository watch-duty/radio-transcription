# =============================================================================
# MESSAGE QUEUES MODULE
# =============================================================================

locals {
  protos_dir = "${path.module}/../../../protos"
}

# =============================================================================
# SCHEMAS
# =============================================================================

# Incoming raw audio transcriptions
resource "google_pubsub_schema" "transcribed_audio_schema" {
  name       = "transcribed-audio-schema-${var.environment}"
  type       = "PROTOCOL_BUFFER"
  definition = try(file("${local.protos_dir}/transcribed_audio.proto"), "")

  lifecycle {
    precondition {
      condition     = fileexists("${local.protos_dir}/transcribed_audio.proto")
      error_message = "Missing transcribed_audio.proto. Run `mise run flatten-schemas` (or generate the .proto file) before applying this module."
    }
  }
}

# Audio segments that have been processed and evaluated
resource "google_pubsub_schema" "evaluated_transcribed_audio_schema" {
  name       = "evaluated-audio-schema-${var.environment}"
  type       = "PROTOCOL_BUFFER"
  definition = try(file("${local.protos_dir}/evaluated_transcribed_audio.proto"), "")

  lifecycle {
    precondition {
      condition     = fileexists("${local.protos_dir}/evaluated_transcribed_audio.proto")
      error_message = "Missing evaluated_transcribed_audio.proto. Run `mise run flatten-schemas` (or generate the .proto file) before applying this module."
    }
  }
}

# Continuous audio chunks for transcription
resource "google_pubsub_schema" "continuous_audio_schema" {
  name       = "continuous-audio-schema-${var.environment}"
  type       = "PROTOCOL_BUFFER"
  definition = try(file("${local.protos_dir}/continuous_audio.proto"), "")

  lifecycle {
    precondition {
      condition     = fileexists("${local.protos_dir}/continuous_audio.proto")
      error_message = "Missing continuous_audio.proto. Run `mise run flatten-schemas` (or generate the .proto file) before applying this module."
    }
  }
}

# Normalized audio claims checks
resource "google_pubsub_schema" "normalized_audio_schema" {
  name       = "normalized-audio-schema-${var.environment}"
  type       = "PROTOCOL_BUFFER"
  definition = try(file("${local.protos_dir}/normalized_audio.proto"), "")

  lifecycle {
    precondition {
      condition     = fileexists("${local.protos_dir}/normalized_audio.proto")
      error_message = "Missing normalized_audio.proto. Run `mise run flatten-schemas` (or generate the .proto file) before applying this module."
    }
  }
}

# Segmented audio claims checks output by the Dataflow segmentation pipeline
resource "google_pubsub_schema" "segmented_audio_claims_schema" {
  name       = "segmented-audio-claims-schema-${var.environment}"
  type       = "PROTOCOL_BUFFER"
  definition = try(file("${local.protos_dir}/segmented_audio.proto"), "")

  lifecycle {
    precondition {
      condition     = fileexists("${local.protos_dir}/segmented_audio.proto")
      error_message = "Missing segmented_audio.proto. Run `mise run flatten-schemas` (or generate the .proto file) before applying this module."
    }
  }
}

# =============================================================================
# PUBSUB PIPELINE QUEUES
# =============================================================================

# Topic for incoming raw audio transcriptions
resource "google_pubsub_topic" "transcribed_audio" {
  name = "transcribed-audio-${var.environment}"

  schema_settings {
    schema   = google_pubsub_schema.transcribed_audio_schema.id
    encoding = "BINARY"
  }
}

# Topic for normalized audio claims checks
resource "google_pubsub_topic" "normalized_audio" {
  name = "normalized-audio-${var.environment}"

  schema_settings {
    schema   = google_pubsub_schema.normalized_audio_schema.id
    encoding = "BINARY"
  }
}

# Topic for segmented audio (produced by Dataflow segmentation or direct ingestion)
resource "google_pubsub_topic" "segmented_audio" {
  name = "segmented-audio-${var.environment}"

  schema_settings {
    schema   = google_pubsub_schema.segmented_audio_claims_schema.id
    encoding = "BINARY"
  }
}



# Topic for transcription jobs — carries GCS file paths of audio to transcribe
resource "google_pubsub_topic" "continuous_audio" {
  name = "continuous-audio-${var.environment}"

  schema_settings {
    schema   = google_pubsub_schema.continuous_audio_schema.id
    encoding = "BINARY"
  }
}

# Subscription to the continuous audio topic
resource "google_pubsub_subscription" "continuous_audio_sub" {
  name  = "continuous-audio-sub-${var.environment}"
  topic = google_pubsub_topic.continuous_audio.id
}



# Topic for audio segments that have been processed and evaluated
resource "google_pubsub_topic" "evaluated_audio" {
  name = "evaluated-audio-${var.environment}"

  schema_settings {
    schema   = google_pubsub_schema.evaluated_transcribed_audio_schema.id
    encoding = "BINARY"
  }
}

# =============================================================================
# PUBSUB DEAD LETTER QUEUES
# Any message sent to a DLQ with no subscription attached is permanently lost
# =============================================================================

# Dead letter topic for evaluated audio processing failures
resource "google_pubsub_topic" "evaluated_audio_dead_letter" {
  name = "evaluated-audio-dead-letter-${var.environment}"
}

# Subscription to the DLQ for evaluated audio
resource "google_pubsub_subscription" "evaluated_audio_dead_letter_subscription" {
  name  = "evaluated-audio-dead-letter-subscription-${var.environment}"
  topic = google_pubsub_topic.evaluated_audio_dead_letter.name

  message_retention_duration = "604800s" # 7 days
}

# Dead letter topic for transcription processing failures
resource "google_pubsub_topic" "transcribed_audio_dlq" {
  name = "transcribed-audio-dlq-${var.environment}"
}

# Subscription to the DLQ for transcription failures to prevent message loss
resource "google_pubsub_subscription" "transcribed_audio_dlq_subscription" {
  name  = "transcribed-audio-dlq-subscription-${var.environment}"
  topic = google_pubsub_topic.transcribed_audio_dlq.name

  message_retention_duration = "604800s" # 7 days
}

# Dead letter topic for normalization processing failures
resource "google_pubsub_topic" "normalized_audio_dlq" {
  name = "normalized-audio-dlq-${var.environment}"
}

# Subscription to the DLQ for normalization failures to prevent message loss
resource "google_pubsub_subscription" "normalized_audio_dlq_subscription" {
  name  = "normalized-audio-dlq-subscription-${var.environment}"
  topic = google_pubsub_topic.normalized_audio_dlq.name

  message_retention_duration = "604800s" # 7 days
}

# Dead letter topic for segmentation pipeline failures
resource "google_pubsub_topic" "segmented_audio_claims_dlq" {
  name = "segmented-audio-claims-dlq-${var.environment}"
}

# Subscription to the DLQ for segmentation failures to prevent message loss
resource "google_pubsub_subscription" "segmented_audio_claims_dlq_subscription" {
  name  = "segmented-audio-claims-dlq-subscription-${var.environment}"
  topic = google_pubsub_topic.segmented_audio_claims_dlq.name

  message_retention_duration = "604800s" # 7 days
}
