resource "google_alloydb_cluster" "this" {
  project    = var.project_id
  cluster_id = var.cluster_id
  location   = var.region
  labels     = var.labels

  deletion_protection = var.deletion_protection

  network_config {
    network            = var.network_id
    allocated_ip_range = var.allocated_ip_range
  }

  initial_user {
    password = var.initial_user_password
  }

  dynamic "continuous_backup_config" {
    for_each = var.continuous_backup_enabled ? [1] : []
    content {
      enabled              = true
      recovery_window_days = var.continuous_backup_retention_days
    }
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
          hours = var.backup_start_hour
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

  dynamic "query_insights_config" {
    for_each = var.query_insights_enabled ? [1] : []
    content {
      query_string_length     = 1024
      record_application_tags = true
      record_client_address   = true
      query_plans_per_minute  = var.query_insights_query_plans_per_minute
    }
  }

  connection_pool_config {
    enabled = var.connection_pooling_enabled
    flags   = var.connection_pooling_enabled ? var.connection_pooling_flags : null
  }
}
