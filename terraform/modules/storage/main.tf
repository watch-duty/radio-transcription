# =============================================================================
# DATABASES
# =============================================================================

data "google_project" "project" {}

locals {
  project_id     = data.google_project.project.project_id
  project_number = data.google_project.project.number
}

# AlloyDB cluster for persistent feed configuration, transcription, and evaluation data
module "alloydb" {
  source = "../alloydb"

  project_id            = local.project_id
  region                = var.region
  cluster_id            = "radio-transcription-${var.environment}"
  network_id            = var.network_id
  allocated_ip_range    = var.psa_range_name
  initial_user_password = random_password.alloydb_postgres.result

  # Worker user for application-level access
  create_worker_user    = true
  worker_user_password  = random_password.alloydb_worker.result
  worker_database_roles = ["alloydbsuperuser"]

  # Instance configuration
  instance_id       = "primary"
  machine_cpu_count = 2
  availability_type = "ZONAL"

  # pg_cron extension: required for the feeds-abandoned-lease-sweep and
  # feeds-vac scheduled jobs in radio-transcription's migration
  # 019_feeds_pg_cron_jobs.sql. Without enable_pg_cron, CREATE EXTENSION
  # pg_cron fails and the schema-apply Cloud Run Job exits non-zero.
  #
  # cron.database_name is pinned to "postgres" defensively. pg_cron's
  # own default is already "postgres" and the alloydb module's
  # schema_database_name default is also "postgres", so today this line
  # is redundant. It's kept explicit so that if anyone later overrides
  # schema_database_name, the mismatch is visible in diff rather than
  # silently scheduling cron.job rows into a database pg_cron won't
  # read.
  #
  # idle_in_transaction_session_timeout = 30000 ms (= 30s) catches dead-
  # mid-transaction ingestion workers holding FOR NO KEY UPDATE row locks
  # on `feeds`. 30s is short enough to release locks inside the scaling
  # plan's 90s envelope with margin, long enough that no legitimate
  # transaction in this codebase comes close: the claim query commits in
  # <1s, heartbeat renewal is a single UPDATE, no application path
  # deliberately sits idle-in-transaction for >1s.
  #
  # IMPORTANT — value must be a plain integer in MILLISECONDS (PG's
  # underlying unit for this GUC). AlloyDB's API rejects PG duration
  # literals like "30s" with 400 INVALID_ARGUMENT ("value '30s' for flag
  # 'idle_in_transaction_session_timeout' is not an integer"), even
  # though Postgres itself would parse "30s" fine via a SET statement.
  # AlloyDB supportedDatabaseFlags lists this flag as valueType=INTEGER.
  # Verified via the validateOnly post-step in Plan (dev) (alloydb.googleapis.com
  # validateOnly=true).
  #
  # The pooler-level socket-idle equivalent (client_idle_timeout) does
  # not exist in AlloyDB's MCP namespace — probed via validateOnly for
  # idle_timeout, client_timeout, server_idle_timeout, tcp_keepalives_
  # idle, client_conn_idle_timeout, autodb_idle_timeout; all rejected.
  # Dead-between-transactions workers hold no locks, so their stale
  # client slots are a non-issue operationally.
  #
  # OPERATIONAL NOTE: alloydb.enable_pg_cron and cron.database_name are
  # "Instance restarts: Yes" per AlloyDB supported-flags documentation.
  # First apply of the pg_cron pair restarted the primary instance
  # (~60s of ZONAL downtime, already applied). idle_in_transaction_
  # session_timeout is a standard PG GUC with context: user and applies
  # live with no instance restart.
  database_flags = {
    "alloydb.enable_pg_cron"              = "on"
    "cron.database_name"                  = "postgres"
    "idle_in_transaction_session_timeout" = "30000"
  }

  # Per-env pooler sizing. Empirically-verified AlloyDB MCP flag names
  # (pool_mode, max_pool_size, max_client_connections — NOT
  # pgbouncer-canonical names; see the alloydb module's
  # variables.tf comment and the validateOnly post-step in Plan (dev) of
  # deploy.yml).
  #
  # max_pool_size (backend connections per pool):
  #   - prod: 160 — sized for the radio-transcription scaling plan §6 peak
  #     fleet (16 VMs × 2 workers × ~4 concurrent txns ≈ 128 backends, plus
  #     heartbeat spikes and API-service slack).
  #   - dev:  40  — ~3-4× the realistic dev peak (one feed per source, ~3
  #     sources, ~4 concurrent txns ≈ 12 backends). Gives headroom for 4
  #     API services × asyncpg pool of 8 each (shared `worker` user) without
  #     oversubscribing the pooler. Room to tighten if dev gets leaner.
  #
  # Why not the bare module-default 8: the shared module default stays
  # conservative for hypothetical future callers; the real dev sizing
  # belongs at this call site where dev's workload shape is known.
  #
  # max_client_connections = 800 and pool_mode = transaction are flat across
  # envs — liberal client-slot cap and the required multiplexing mode that
  # work everywhere. Set explicitly here (vs. inherited from the module
  # default) so the whole pooler config is visible at one call site.
  #
  # Note on dead-client detection: AlloyDB's MCP namespace does not expose
  # a client_idle_timeout / idle_timeout equivalent (verified via
  # validateOnly probes against the API). Dead-mid-transaction workers are
  # handled by the idle_in_transaction_session_timeout = "30s" GUC in
  # database_flags above; dead-between-transactions workers hold no locks,
  # so their stale client slots don't matter operationally.
  #
  # The map is a full replacement (Terraform map(string) semantics) — every
  # key must be passed; omitting any falls back to the module default.
  connection_pooling_flags = {
    pool_mode              = "transaction"
    max_pool_size          = var.environment == "prod" ? "160" : "40"
    max_client_connections = "800"
  }

  # Automatic schema application
  apply_schema       = true
  password_secret_id = google_secret_manager_secret.alloydb_postgres_password.secret_id
  subnetwork_id      = var.subnet_id

  labels = {
    environment = var.environment
    service     = "radio-transcription"
    managed_by  = "terraform"
  }

  depends_on = [
    google_secret_manager_secret_version.alloydb_postgres_password,
  ]
}

# Memory for Redis instance for the notification pipeline message deduplication.
module "notification_pipeline_redis" {
  # tflint-ignore: terraform_module_pinned_source
  source = "../memorystore_for_redis"

  name         = "notification-deduplication-${var.environment}"
  display_name = "Notification Pipeline Message Deduplication"
  region       = var.region
  project_id   = local.project_id

  network_id   = var.network_id
  connect_mode = "PRIVATE_SERVICE_ACCESS"
  # Standard has cross-region support, better availability/read throughput, and no data loss, compared to Basic tier.
  tier = "STANDARD_HA"
  # Disabling read replicas while still in development. Cost is 4x with enabled which also requires >= 5 GiB of memory size allocated.
  read_replicas_mode = "READ_REPLICAS_DISABLED"
  replica_count      = 1
  memory_size_gb     = 1

  auth_enabled            = true
  transit_encryption_mode = "SERVER_AUTHENTICATION"
}

# =============================================================================
# CREDENTIALS
# =============================================================================

# -----------------------------------------------------------------------------
# AlloyDB
# -----------------------------------------------------------------------------

resource "random_password" "alloydb_postgres" {
  length  = 32
  special = true
}

# Randomly generated password for the AlloyDB worker user
resource "random_password" "alloydb_worker" {
  length  = 32
  special = true
}

# Secret Manager resource for the postgres password
resource "google_secret_manager_secret" "alloydb_postgres_password" {
  project   = local.project_id
  secret_id = "alloydb-postgres-password-${var.environment}"
  replication {
    auto {}
  }
}

# Store the generated postgres password as a secret version
resource "google_secret_manager_secret_version" "alloydb_postgres_password" {
  secret      = google_secret_manager_secret.alloydb_postgres_password.id
  secret_data = random_password.alloydb_postgres.result
}

# Secret Manager resource for the worker password
resource "google_secret_manager_secret" "alloydb_worker_password" {
  project   = local.project_id
  secret_id = "alloydb-worker-password-${var.environment}"
  replication {
    auto {}
  }
}

# Store the generated worker password as a secret version
resource "google_secret_manager_secret_version" "alloydb_worker_password" {
  secret      = google_secret_manager_secret.alloydb_worker_password.id
  secret_data = random_password.alloydb_worker.result
}

# -----------------------------------------------------------------------------
# Redis
# -----------------------------------------------------------------------------

# Secret Manager resource for the Memorystore AUTH string password.
resource "google_secret_manager_secret" "notification_pipeline_redis_password" {
  project   = local.project_id
  secret_id = "notification-pipeline-redis-password"
  replication {
    auto {}
  }
}

# Store the AUTH string password output from Memorystore as a secret version.
resource "google_secret_manager_secret_version" "notification_pipeline_redis_password" {
  secret      = google_secret_manager_secret.notification_pipeline_redis_password.id
  secret_data = module.notification_pipeline_redis.password
}

# Secret Manager resource for the Memorystore CA certificate.
resource "google_secret_manager_secret" "notification_pipeline_redis_certificate" {
  project   = local.project_id
  secret_id = "notification-pipeline-redis-certificate-${var.environment}"
  replication {
    auto {}
  }
}

# Store the CA certificate output from Memorystore as a secret version.
resource "google_secret_manager_secret_version" "notification_pipeline_redis_certificate" {
  secret      = google_secret_manager_secret.notification_pipeline_redis_certificate.id
  secret_data = module.notification_pipeline_redis.certificate
}

# =============================================================================
# GCS BUCKETS
# =============================================================================

# Staging bucket for Dataflow temporary files
module "dataflow_staging_bucket" {
  source = "../gcs_bucket"

  project_id         = local.project_id
  name               = "${local.project_id}-dataflow-staging-${var.environment}"
  location           = var.region
  enable_soft_delete = false # High-churn temp bucket — soft-delete adds unnecessary cost

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 30 # Temporary files only need to live 30 days
      }
    }
  ]
}

module "ingestion_staging_bucket" {
  source = "../gcs_bucket"

  project_id = local.project_id
  name       = var.ingestion_staging_bucket_name
  location   = var.region

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age = 1095 # 3 years
      }
    }
  ]

  labels = {
    environment = var.environment
    service     = "radio-transcription"
    managed_by  = "terraform"
  }
}

module "ingestion_canonical_bucket" {
  source = "../gcs_bucket"

  project_id = local.project_id
  name       = var.ingestion_canonical_bucket_name
  location   = var.region

  labels = {
    environment = var.environment
    service     = "radio-transcription"
    managed_by  = "terraform"
  }

  cors = [{
    origin          = [var.web_domain]
    method          = ["OPTIONS", "GET"]
    response_header = ["Content-Length", "Content-Type", "Access-Control-Allow-Origin"]
    max_age_seconds = 3600 # 1 hour
  }]

  lifecycle_rules = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        age            = 14 # Diagnostic ephemeral audio lives 14 days (2 weeks)
        matches_prefix = ["ephemeral/"]
      }
    }
  ]
}

resource "google_storage_bucket_iam_member" "ingestion_canonical_bucket_public_read" {
  bucket = module.ingestion_canonical_bucket.name
  role   = "roles/storage.objectViewer"
  member = "allUsers"
}

# Grant the Cloud Speech-to-Text Service Agent read access to the canonical bucket.
# Required for the serverless transcription service to transcribe audio files
# stored in GCS using the Speech-to-Text API.
resource "google_storage_bucket_iam_member" "speech_service_agent_canonical_bucket_reader" {
  bucket = module.ingestion_canonical_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:service-${local.project_number}@gcp-sa-speech.iam.gserviceaccount.com"
}
