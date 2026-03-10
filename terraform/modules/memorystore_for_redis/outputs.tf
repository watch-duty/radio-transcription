# This is required to connect to the instance.
output "host" {
  description = "The IP address of the instance."
  value       = google_redis_instance.this.host
}

output "port" {
  description = "The port of the instance."
  value = google_redis_instance.this.port
}
