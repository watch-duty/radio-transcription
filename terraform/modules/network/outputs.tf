output "network_id" {
  description = "The ID of the VPC network."
  value       = google_compute_network.main.id
}

output "network_name" {
  description = "The name of the VPC network."
  value       = google_compute_network.main.name
}

output "subnet_id" {
  description = "The ID of the subnetwork."
  value       = google_compute_subnetwork.main.id
}

output "subnet_name" {
  description = "The name of the subnetwork."
  value       = google_compute_subnetwork.main.name
}

output "psa_range_name" {
  description = "The name of the private service access range."
  value       = google_compute_global_address.private_ip_alloc.name
}
