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
# Applies the privilege bootstrap, ordered ingestion DDL, and privilege
# reconciliation to the AlloyDB instance after it is provisioned. A Cloud Run
# Job was chosen over a null_resource + psql because AlloyDB uses private IP
# only — the job runs inside the VPC with Direct VPC egress, eliminating the
# need for a self-hosted runner or VPN.
#
# Flow: SQL files are uploaded to GCS, mounted into the caller-pinned immutable
# PostgreSQL client image via GCS FUSE, and executed through one psql process
# per file. The job is triggered by a null_resource whose trigger hash changes
# whenever the SQL content changes, ensuring re-application on schema updates.
# All SQL is idempotent (IF NOT EXISTS / ON CONFLICT) so re-runs are safe.
#
# All resources in this section are conditionally created; setting
# apply_schema = false (the default) skips them entirely, keeping the module
# backward-compatible for consumers that only need the cluster/instance.
# -----------------------------------------------------------------------------

locals {
  active_primary_instance_ip = coalesce(var.active_primary_instance_ip, google_alloydb_instance.primary.ip_address)
  schema_sql_files           = var.apply_schema ? sort(fileset("${path.module}/sql/ingestion", "*.sql")) : []
  bcfy_calls_sid_operation_files = var.apply_schema ? sort(fileset(
    "${path.module}/sql/operations/bcfy_calls_sid",
    "*.sql",
  )) : []
  privilege_sql_files = var.apply_schema ? [
    "000_ingestion_runtime_bootstrap.sql",
    "100_ingestion_runtime_hardening.sql",
    "999_ingestion_runtime_reconcile.sql",
  ] : []
  schema_sql_combined = join("\n", concat(
    [for f in local.privilege_sql_files : file("${path.module}/sql/privileges/${f}") if f == "000_ingestion_runtime_bootstrap.sql"],
    [for f in local.privilege_sql_files : file("${path.module}/sql/privileges/${f}") if f == "100_ingestion_runtime_hardening.sql"],
    [for f in local.schema_sql_files : file("${path.module}/sql/ingestion/${f}")],
    [for f in local.privilege_sql_files : file("${path.module}/sql/privileges/${f}") if f == "999_ingestion_runtime_reconcile.sql"],
  ))
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

  name   = "ingestion/${each.value}"
  bucket = google_storage_bucket.schema[0].name
  source = "${path.module}/sql/ingestion/${each.value}"
}

resource "google_storage_bucket_object" "privilege_sql" {
  for_each = var.apply_schema ? toset(local.privilege_sql_files) : toset([])

  name   = "privileges/${each.value}"
  bucket = google_storage_bucket.schema[0].name
  source = "${path.module}/sql/privileges/${each.value}"
}

# Controlled authority operations are uploaded under a disjoint prefix.  They
# are deliberately excluded from schema_sql_files/schema_sql_hash and cannot
# be reached by execute_schema_migration.
resource "google_storage_bucket_object" "bcfy_calls_sid_operation" {
  for_each = var.apply_schema ? toset(local.bcfy_calls_sid_operation_files) : toset([])

  name   = "operations/bcfy_calls_sid/${each.value}"
  bucket = google_storage_bucket.schema[0].name
  source = "${path.module}/sql/operations/bcfy_calls_sid/${each.value}"
}

resource "google_storage_bucket_object" "ingestion_lease_runtime_check" {
  count = var.apply_schema ? 1 : 0

  name   = "ci/ingestion_lease_runtime_columns_check.sql"
  bucket = google_storage_bucket.schema[0].name
  source = "${path.module}/sql/ci/ingestion_lease_runtime_columns_check.sql"
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

# The migration job itself. Uses the caller-pinned immutable image for the psql
# client, connects to AlloyDB over the private VPC via Direct VPC egress, and
# reads one caller-pinned numeric database-password version from Secret Manager.
# The plaintext is absent from Terraform state and the Job template; Cloud Run
# resolves it into the task's runtime environment, which must never be dumped.
# SQL files are mounted read-only from GCS.
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
    precondition {
      condition     = var.sql_job_image != null
      error_message = "sql_job_image must be provided when apply_schema is true."
    }
    precondition {
      condition     = var.password_secret_version != null
      error_message = "password_secret_version must be provided when apply_schema is true."
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
        image   = var.sql_job_image
        command = ["/bin/sh"]
        args = [
          "-c",
          "set -eu; export LC_ALL=C; echo 'Applying ingestion privilege bootstrap...'; psql -X -v ON_ERROR_STOP=1 -v legacy_role=\"$DB_LEGACY_ROLE\" -f /sql/privileges/000_ingestion_runtime_bootstrap.sql -h \"$DB_HOST\" -p \"$DB_PORT\" -U \"$DB_USER\" -d \"$DB_NAME\"; for f in /sql/ingestion/*.sql; do echo \"Applying $f...\"; psql -X -v ON_ERROR_STOP=1 -f \"$f\" -h \"$DB_HOST\" -p \"$DB_PORT\" -U \"$DB_USER\" -d \"$DB_NAME\"; done; echo 'Reconciling ingestion runtime privileges...'; psql -X -v ON_ERROR_STOP=1 -v legacy_role=\"$DB_LEGACY_ROLE\" -f /sql/privileges/999_ingestion_runtime_reconcile.sql -h \"$DB_HOST\" -p \"$DB_PORT\" -U \"$DB_USER\" -d \"$DB_NAME\"; echo 'Schema and privileges applied successfully.'"
        ]

        env {
          name  = "DB_HOST"
          value = local.active_primary_instance_ip
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
          name  = "DB_LEGACY_ROLE"
          value = var.create_worker_user ? var.worker_user_id : ""
        }
        env {
          name = "PGPASSWORD"
          value_source {
            secret_key_ref {
              secret  = var.password_secret_id
              version = var.password_secret_version
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
    google_alloydb_user.worker,
    google_secret_manager_secret_iam_member.schema_migrator,
    google_storage_bucket_iam_member.schema_migrator,
  ]
}

# Dormant, explicitly invoked surface for the reviewed SID authority handoff.
# The immutable command accepts only four operation names.  Execution-time
# argument overrides can select an operation and its reviewed scalar inputs,
# but can never select an arbitrary path or shell command.
resource "google_cloud_run_v2_job" "bcfy_calls_sid_operation" {
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
    precondition {
      condition     = var.sql_job_image != null
      error_message = "sql_job_image must be provided when apply_schema is true."
    }
    precondition {
      condition     = var.password_secret_version != null
      error_message = "password_secret_version must be provided when apply_schema is true."
    }
  }

  name                = "${var.cluster_id}-bcfy-calls-sid-operation"
  location            = var.region
  project             = var.project_id
  deletion_protection = false

  template {
    task_count = 1

    template {
      service_account = google_service_account.schema_migrator[0].email
      timeout         = "300s"
      max_retries     = 0

      containers {
        image = var.sql_job_image
        command = [
          "/bin/sh",
          "-c",
          <<-EOT
            set -eu
            export LC_ALL=C
            operation="$1"
            run_sql() {
              psql -X -v ON_ERROR_STOP=1 \
                -h "$DB_HOST" -p "$DB_PORT" \
                -U "$DB_USER" -d "$DB_NAME" "$@"
            }
            case "$operation" in
              verify)
                [ "$#" -eq 1 ] || exit 64
                operation_file=/sql/operations/bcfy_calls_sid/004_verify.sql
                run_sql -f "$operation_file"
                ;;
              preseed)
                [ "$#" -eq 1 ] || exit 64
                operation_file=/sql/operations/bcfy_calls_sid/001_preseed.sql
                run_sql -f "$operation_file"
                ;;
              activate)
                [ "$#" -eq 4 ] || exit 64
                operation_file=/sql/operations/bcfy_calls_sid/002_activate.sql
                run_sql \
                  -v reviewed_sid_count="$2" \
                  -v reviewed_manifest_digest="$3" \
                  -v process_absence_confirmed="$4" \
                  -f "$operation_file"
                ;;
              rollback_children)
                [ "$#" -eq 2 ] || exit 64
                operation_file=/sql/operations/bcfy_calls_sid/003_rollback_children.sql
                run_sql \
                  -v process_absence_confirmed="$2" \
                  -f "$operation_file"
                ;;
              *)
                echo "unsupported bcfy_calls SID operation" >&2
                exit 64
                ;;
            esac
          EOT
        ]
        # /bin/sh -c consumes the first argument as $0.  The default is a
        # read-only verification; operators may override only these arguments.
        args = ["bcfy-calls-sid-operation", "verify"]

        env {
          name  = "DB_HOST"
          value = local.active_primary_instance_ip
        }
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
              version = var.password_secret_version
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
    google_service_account.schema_migrator,
    google_secret_manager_secret_iam_member.schema_migrator,
    google_storage_bucket_iam_member.schema_migrator,
    google_storage_bucket_object.bcfy_calls_sid_operation,
    google_storage_bucket_object.ingestion_lease_runtime_check,
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
    schema_hash           = local.schema_sql_hash
    legacy_role           = var.create_worker_user ? var.worker_user_id : ""
    legacy_database_roles = jsonencode(sort(var.worker_database_roles))
  }

  provisioner "local-exec" {
    command = "gcloud run jobs execute ${google_cloud_run_v2_job.schema_migration[0].name} --region=${var.region} --project=${var.project_id} --wait"
  }

  depends_on = [
    google_cloud_run_v2_job.schema_migration,
    google_alloydb_user.worker,
    google_storage_bucket_object.privilege_sql,
    google_storage_bucket_object.sql,
  ]
}
