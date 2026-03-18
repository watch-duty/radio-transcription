# This is required to connect to the instance.
output "host" {
  description = "The IP address of the instance."
  value       = google_redis_instance.this.host
}

output "port" {
  description = "The port of the instance."
  value       = google_redis_instance.this.port
}

output "password" {
  description = "AUTH string required to authenticate when connecting to the instance."
  value       = google_redis_instance.this.auth_string
  sensitive   = true # Hidden from terraform logging
}

output "certificates" {
  description = "TLS certificates in PEM file format."
  value       = google_redis_instance.this.server_ca_certs
  sensitive   = true
}
