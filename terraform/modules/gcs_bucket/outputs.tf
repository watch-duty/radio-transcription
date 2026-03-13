output "name" {
  description = "The name of the bucket."
  value       = google_storage_bucket.this.name
}

output "url" {
  description = "The gs:// URL of the bucket."
  value       = "gs://${google_storage_bucket.this.name}"
}

output "self_link" {
  description = "The URI of the bucket."
  value       = google_storage_bucket.this.self_link
}
