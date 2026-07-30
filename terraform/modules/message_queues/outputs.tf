# =============================================================================
# MAIN PIPELINE TOPICS
# =============================================================================

output "topic_transcribed_audio_id" {
  description = "The ID of the transcribed audio topic."
  value       = google_pubsub_topic.transcribed_audio.id
}

output "topic_transcribed_audio_name" {
  description = "The name of the transcribed audio topic."
  value       = google_pubsub_topic.transcribed_audio.name
}

output "topic_normalized_audio_id" {
  description = "The ID of the normalized audio topic."
  value       = google_pubsub_topic.normalized_audio.id
}

output "topic_normalized_audio_name" {
  description = "The name of the normalized audio topic."
  value       = google_pubsub_topic.normalized_audio.name
}

output "topic_segmented_audio_id" {
  description = "The ID of the segmented audio topic."
  value       = google_pubsub_topic.segmented_audio.id
}

output "topic_segmented_audio_name" {
  description = "The name of the segmented audio topic."
  value       = google_pubsub_topic.segmented_audio.name
}

output "topic_continuous_audio_id" {
  description = "The ID of the continuous audio topic."
  value       = google_pubsub_topic.continuous_audio.id
}

output "topic_continuous_audio_name" {
  description = "The name of the continuous audio topic."
  value       = google_pubsub_topic.continuous_audio.name
}

output "topic_evaluated_audio_id" {
  description = "The ID of the evaluated audio topic."
  value       = google_pubsub_topic.evaluated_audio.id
}

output "topic_evaluated_audio_name" {
  description = "The name of the evaluated audio topic."
  value       = google_pubsub_topic.evaluated_audio.name
}

# =============================================================================
# DEAD LETTER QUEUE TOPICS
# =============================================================================

output "topic_evaluated_audio_dead_letter_id" {
  description = "The ID of the evaluated audio dead letter topic."
  value       = google_pubsub_topic.evaluated_audio_dead_letter.id
}

output "topic_evaluated_audio_dead_letter_name" {
  description = "The name of the evaluated audio dead letter topic."
  value       = google_pubsub_topic.evaluated_audio_dead_letter.name
}

output "topic_transcribed_audio_dlq_id" {
  description = "The ID of the transcribed audio dead letter queue topic."
  value       = google_pubsub_topic.transcribed_audio_dlq.id
}

output "topic_transcribed_audio_dlq_name" {
  description = "The name of the transcribed audio dead letter queue topic."
  value       = google_pubsub_topic.transcribed_audio_dlq.name
}

output "topic_normalized_audio_dlq_id" {
  description = "The ID of the normalized audio dead letter queue topic."
  value       = google_pubsub_topic.normalized_audio_dlq.id
}

output "topic_normalized_audio_dlq_name" {
  description = "The name of the normalized audio dead letter queue topic."
  value       = google_pubsub_topic.normalized_audio_dlq.name
}

output "topic_segmented_audio_claims_dlq_id" {
  description = "The ID of the segmented audio claims dead letter queue topic."
  value       = google_pubsub_topic.segmented_audio_claims_dlq.id
}

output "topic_segmented_audio_claims_dlq_name" {
  description = "The name of the segmented audio claims dead letter queue topic."
  value       = google_pubsub_topic.segmented_audio_claims_dlq.name
}

output "subscription_segmented_audio_claims_dlq_name" {
  description = "The name of the segmented audio claims dead letter queue subscription."
  value       = google_pubsub_subscription.segmented_audio_claims_dlq_subscription.name
}

