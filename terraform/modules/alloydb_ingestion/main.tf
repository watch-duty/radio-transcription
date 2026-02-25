resource "google_alloydb_cluster" "this" {
  project    = var.project_id
  cluster_id = var.cluster_id
  location   = var.region
  labels     = var.labels

  network_config {
    network            = var.network_id
    allocated_ip_range = var.allocated_ip_range
  }

  initial_user {
    password = var.initial_user_password
  }

  dynamic "automated_backup_policy" {
    for_each = var.automated_backup_enabled ? [1] : []
    content {
      enabled       = true
      backup_window = var.backup_window
      location      = var.region

      weekly_schedule {
        days_of_week = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]
        start_times {
          hours = 2
        }
      }

      quantity_based_retention {
        count = var.backup_retention_count
      }
    }
  }
}

resource "google_alloydb_instance" "primary" {
  cluster       = google_alloydb_cluster.this.name
  instance_id   = var.instance_id
  instance_type = "PRIMARY"
  labels        = var.labels

  machine_config {
    cpu_count = var.machine_cpu_count
  }

  availability_type = var.availability_type
  database_flags    = var.database_flags

  connection_pool_config {
    enabled = var.connection_pooling_enabled
    flags   = var.connection_pooling_enabled ? var.connection_pooling_flags : null
  }
}
