output "instance_ip" {
  description = "The public IP address of the evaluation instance"
  value       = google_compute_instance.eval_instance.network_interface[0].access_config[0].nat_ip
}

output "instance_name" {
  description = "The name of the evaluation instance"
  value       = google_compute_instance.eval_instance.name
}
