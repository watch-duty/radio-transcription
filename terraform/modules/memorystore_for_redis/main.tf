resource "google_redis_instance" "this" {
  name         = var.name
  display_name = var.display_name
  region       = var.region
  project      = var.project_id

  memory_size_gb     = var.memory_size_gb
  tier               = var.tier
  read_replicas_mode = var.read_replicas_mode
  replica_count      = var.replica_count

  authorized_network = var.network_id
  connect_mode       = var.connect_mode

  auth_enabled            = var.auth_enabled
  transit_encryption_mode = var.transit_encryption_mode

  lifecycle {
    prevent_destroy = true
  }
}
