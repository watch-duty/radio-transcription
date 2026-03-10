output "instance_group_manager_id" {
  description = "The full resource ID of the managed instance group."
  value       = google_compute_instance_group_manager.this.id
}

output "instance_group_self_link" {
  description = "The self-link of the instance group (for use in other resources)."
  value       = google_compute_instance_group_manager.this.instance_group
}

output "instance_template_id" {
  description = "The full resource ID of the instance template."
  value       = google_compute_instance_template.this.id
}
