# =============================================================================
# DATABASES
# =============================================================================

# AlloyDB cluster for persistent feed configuration, transcription, and evaluation data
module "alloydb" {
  source = "../alloydb"

  project_id            = var.project_id
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

  database_flags = {
    "alloydb.enable_pg_cron"              = "on"
    "cron.database_name"                  = "postgres"
    "idle_in_transaction_session_timeout" = "30000"
  }

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
  source = "../memorystore_for_redis"

  name         = "notification-deduplication-${var.environment}"
  display_name = "Notification Pipeline Message Deduplication"
  region       = var.region
  project_id   = var.project_id

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
  project   = var.project_id
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
  project   = var.project_id
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
  project   = var.project_id
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
  project   = var.project_id
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

  project_id         = var.project_id
  name               = "${var.project_id}-dataflow-staging-${var.environment}"
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

  project_id = var.project_id
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

  project_id = var.project_id
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

data "google_project" "project" {}

# Grant the Cloud Speech-to-Text Service Agent read access to the canonical bucket.
# Required for the serverless transcription service to transcribe audio files
# stored in GCS using the Speech-to-Text API.
resource "google_storage_bucket_iam_member" "speech_service_agent_canonical_bucket_reader" {
  bucket = module.ingestion_canonical_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-speech.iam.gserviceaccount.com"
}
