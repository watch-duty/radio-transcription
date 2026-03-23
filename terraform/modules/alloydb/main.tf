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
    flags   = var.connection_pooling_enabled ? var.connection_pooling_flags : {}
  }
}

resource "google_alloydb_user" "worker" {
  count = var.create_worker_user ? 1 : 0

  lifecycle {
    precondition {
      condition     = var.worker_user_password != null
      error_message = "worker_user_password must be provided when create_worker_user is true."
    }
  }

  cluster        = google_alloydb_cluster.this.name
  user_id        = var.worker_user_id
  user_type      = "ALLOYDB_BUILT_IN"
  password       = var.worker_user_password
  database_roles = var.worker_database_roles

  depends_on = [google_alloydb_instance.primary]
}

# -----------------------------------------------------------------------------
# Schema Application via Cloud Run Job (gated behind var.apply_schema)
#
# Applies the ingestion DDL (sql/ingestion/*.sql) to the AlloyDB instance after
# it is provisioned. A Cloud Run Job was chosen over a null_resource + psql
# because AlloyDB uses private IP only — the job runs inside the VPC with
# Direct VPC egress, eliminating the need for a self-hosted runner or VPN.
#
# Flow: SQL files are uploaded to GCS, mounted into a postgres:16-alpine
# container via GCS FUSE, and executed in filename order via psql. The job is
# triggered by a null_resource whose trigger hash changes whenever the SQL
# content changes, ensuring re-application on schema updates. All SQL is
# idempotent (IF NOT EXISTS / ON CONFLICT) so re-runs are safe.
#
# All resources in this section are conditionally created; setting
# apply_schema = false (the default) skips them entirely, keeping the module
# backward-compatible for consumers that only need the cluster/instance.
# -----------------------------------------------------------------------------

locals {
  schema_sql_files = var.apply_schema ? sort(fileset("${path.module}/sql/ingestion", "*.sql")) : []
  schema_sql_combined = join("\n", [
    for f in local.schema_sql_files : file("${path.module}/sql/ingestion/${f}")
  ])
  schema_sql_hash = var.apply_schema ? sha256(local.schema_sql_combined) : ""
}

# Staging bucket for SQL migration files. The Cloud Run Job mounts this bucket
# via GCS FUSE to access the DDL files at runtime. force_destroy is safe here
# because only Terraform-managed SQL files are stored in this bucket.
resource "google_storage_bucket" "schema" {
  count = var.apply_schema ? 1 : 0

  name                        = "${var.project_id}-alloydb-schema"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

resource "google_storage_bucket_object" "sql" {
  for_each = var.apply_schema ? toset(local.schema_sql_files) : toset([])

  name   = each.value
  bucket = google_storage_bucket.schema[0].name
  source = "${path.module}/sql/ingestion/${each.value}"
}

# Dedicated service account for the Cloud Run Job, scoped to only the
# permissions it needs: reading the database password from Secret Manager
# and reading SQL files from the GCS staging bucket.
resource "google_service_account" "schema_migrator" {
  count = var.apply_schema ? 1 : 0

  project      = var.project_id
  account_id   = "alloydb-schema-migrator"
  display_name = "AlloyDB Schema Migrator"
}

resource "google_secret_manager_secret_iam_member" "schema_migrator" {
  count = var.apply_schema ? 1 : 0

  secret_id = "projects/${var.project_id}/secrets/${var.password_secret_id}"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.schema_migrator[0].email}"
}

resource "google_storage_bucket_iam_member" "schema_migrator" {
  count = var.apply_schema ? 1 : 0

  bucket = google_storage_bucket.schema[0].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.schema_migrator[0].email}"
}

# The migration job itself. Uses postgres:16-alpine for the psql client,
# connects to AlloyDB over the private VPC via Direct VPC egress, and reads
# the database password from Secret Manager at runtime (never stored in
# Terraform state or container env). SQL files are mounted read-only from GCS.
resource "google_cloud_run_v2_job" "schema_migration" {
  count = var.apply_schema ? 1 : 0

  lifecycle {
    precondition {
      condition     = var.password_secret_id != null
      error_message = "password_secret_id must be provided when apply_schema is true."
    }
    precondition {
      condition     = var.subnetwork_id != null
      error_message = "subnetwork_id must be provided when apply_schema is true."
    }
  }

  name                = "${var.cluster_id}-schema-migration"
  location            = var.region
  project             = var.project_id
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.schema_migrator[0].email
      timeout         = "300s"
      max_retries     = 1

      containers {
        image   = "postgres:16-alpine"
        command = ["/bin/sh"]
        args = [
          "-c",
          "for f in $(ls /sql/*.sql | sort); do echo \"Applying $f...\"; PGPASSWORD=\"$PGPASSWORD\" psql -h \"$DB_HOST\" -p \"$DB_PORT\" -U \"$DB_USER\" -d \"$DB_NAME\" -v ON_ERROR_STOP=1 -f \"$f\" || exit 1; done; echo 'Schema applied successfully.'"
        ]

        env {
          name  = "DB_HOST"
          value = google_alloydb_instance.primary.ip_address
        }
        # Direct port (5432), not the managed pooler (6432). DDL statements
        # must bypass PgBouncer transaction-mode pooling.
        env {
          name  = "DB_PORT"
          value = "5432"
        }
        env {
          name  = "DB_USER"
          value = "postgres"
        }
        env {
          name  = "DB_NAME"
          value = var.schema_database_name
        }
        env {
          name = "PGPASSWORD"
          value_source {
            secret_key_ref {
              secret  = var.password_secret_id
              version = "latest"
            }
          }
        }

        volume_mounts {
          name       = "sql-files"
          mount_path = "/sql"
        }
      }

      volumes {
        name = "sql-files"
        gcs {
          bucket    = google_storage_bucket.schema[0].name
          read_only = true
        }
      }

      vpc_access {
        network_interfaces {
          network    = var.network_id
          subnetwork = var.subnetwork_id
        }
      }
    }
  }

  depends_on = [
    google_alloydb_instance.primary,
    google_secret_manager_secret_iam_member.schema_migrator,
    google_storage_bucket_iam_member.schema_migrator,
  ]
}

# Triggers the Cloud Run Job whenever the SQL file content changes. The hash
# of all SQL files is used as the trigger — when any file is added, removed,
# or modified, Terraform re-creates this resource, which executes the job.
# The --wait flag blocks until the job completes, so Terraform can detect
# failures. Idempotent SQL means re-runs are always safe.
resource "null_resource" "execute_schema_migration" {
  count = var.apply_schema ? 1 : 0

  triggers = {
    schema_hash = local.schema_sql_hash
  }

  provisioner "local-exec" {
    command = "gcloud run jobs execute ${google_cloud_run_v2_job.schema_migration[0].name} --region=${var.region} --project=${var.project_id} --wait"
  }

  depends_on = [
    google_cloud_run_v2_job.schema_migration,
    google_storage_bucket_object.sql,
  ]
}
