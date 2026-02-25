output "cluster_id" {
  description = "The full resource name of the AlloyDB cluster."
  value       = google_alloydb_cluster.this.name
}

output "cluster_name" {
  description = "The cluster identifier string."
  value       = google_alloydb_cluster.this.cluster_id
}

output "primary_instance_id" {
  description = "The full resource name of the primary instance."
  value       = google_alloydb_instance.primary.name
}

output "primary_instance_name" {
  description = "The instance identifier string."
  value       = google_alloydb_instance.primary.instance_id
}

output "primary_instance_ip" {
  description = "The private IP address of the primary instance (connect on port 5432 for direct, 6432 for pooled)."
  value       = google_alloydb_instance.primary.ip_address
}

output "connection_pooling_port" {
  description = "The port for Managed Connection Pooling connections. Workers should connect to primary_instance_ip on this port."
  value       = 6432
}

output "worker_user_id" {
  description = "The username of the dedicated worker fleet user, or null if not created."
  value       = var.create_worker_user ? google_alloydb_user.worker[0].user_id : null
}
