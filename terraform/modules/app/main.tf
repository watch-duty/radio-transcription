# =============================================================================
# RADIO TRANSCRIPTION INFRASTRUCTURE (APP MODULE)
# =============================================================================
#
# File layout:
# 1. Project Foundation
# 2. Networking
# 3. Database (AlloyDB / Memorystore for Redis)
# 4. Messaging (Pub/Sub)
# 5. Transcription Pipeline
# 6. API Services
# 7. Internal Tooling & Assets
# 8. Monitoring & Alerting
#
# NOTE: This file is organized top-down by platform concerns
# (foundation -> data plane -> IAM -> runtime services -> operational tooling).
# =============================================================================

# =============================================================================
# 1. PROJECT FOUNDATION
# =============================================================================

data "google_project" "project" {}

data "google_compute_image" "cos" {
  family  = "cos-stable"
  project = "cos-cloud"
}

data "google_compute_zones" "available" {
  project = var.project_id
  region  = var.region
}

locals {
  deployer_sa_email = var.wif_service_account
  deployer_iam_enabled = (
    local.deployer_sa_email != null
    && trimspace(local.deployer_sa_email) != ""
  )
  feed_change_webhook_api_key = (
    trimspace(var.feed_change_webhook_api_key) != ""
    ? var.feed_change_webhook_api_key
    : var.external_endpoint_api_key
  )
}

# Enable required Google Cloud APIs for the project

resource "google_project_service" "apis" {
  for_each = toset([
    "alloydb.googleapis.com",
    "compute.googleapis.com",
    "servicenetworking.googleapis.com",
    "secretmanager.googleapis.com",
    "run.googleapis.com",
    "iam.googleapis.com",
    "artifactregistry.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "speech.googleapis.com",
    "eventarc.googleapis.com",
    "cloudbuild.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "iap.googleapis.com",
    "redis.googleapis.com",
    "vpcaccess.googleapis.com",
    "cloudscheduler.googleapis.com",
    "datalineage.googleapis.com",
    "dataplex.googleapis.com",
    "datacatalog.googleapis.com"
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# =============================================================================
# 2. NETWORKING
# =============================================================================

# Primary VPC network for the transcription system
module "network" {
  source      = "../network"
  project_id  = var.project_id
  region      = var.region
  environment = var.environment

  depends_on = [google_project_service.apis]
}

# =============================================================================
# 3. DATABASE (ALLOYDB / MEMORYSTORE FOR REDIS)
# =============================================================================

module "storage" {
  source     = "../storage"
  depends_on = [google_project_service.apis]

  region                          = var.region
  environment                     = var.environment
  network_id                      = module.network.network_id
  subnet_id                       = module.network.subnet_id
  psa_range_name                  = module.network.psa_range_name
  ingestion_staging_bucket_name   = var.ingestion_staging_bucket_name
  ingestion_canonical_bucket_name = var.ingestion_canonical_bucket_name
  web_domain                      = var.web_domain
}

# =============================================================================
# 4. MESSAGING (PUB/SUB)
# =============================================================================

module "message_queues" {
  source      = "../message_queues"
  depends_on  = [google_project_service.apis]
  environment = var.environment
}

module "feed_change_notifications" {
  count = var.feed_change_notifications_enabled ? 1 : 0

  source = "../feed_change_notifications"

  region                                 = var.region
  environment                            = var.environment
  webhook_url                            = var.feed_change_webhook_destination_url
  webhook_api_key                        = local.feed_change_webhook_api_key
  deployer_service_account_email         = local.deployer_sa_email
  slack_critical_notification_channel_id = var.slack_critical_notification_channel_id

  depends_on = [
    google_project_service.apis,
    module.deployer,
  ]
}

# =============================================================================
# 5. Transcription Pipeline
# =============================================================================

module "ingestion" {
  source = "../services/pipeline/ingestion"

  region                          = var.region
  environment                     = var.environment
  cos_image_self_link             = data.google_compute_image.cos.self_link
  available_zones                 = data.google_compute_zones.available.names
  network_id                      = module.network.network_id
  subnet_id                       = module.network.subnet_id
  broadcastify_username           = var.broadcastify_username
  broadcastify_password           = var.broadcastify_password
  broadcastify_xan_token          = var.broadcastify_xan_token
  broadcastify_api_key            = var.broadcastify_api_key
  broadcastify_api_key_id         = var.broadcastify_api_key_id
  broadcastify_api_app_id         = var.broadcastify_api_app_id
  alloydb_primary_instance_ip     = module.storage.alloydb_primary_instance_ip
  alloydb_worker_password         = module.storage.alloydb_worker_password
  alloydb_connection_pooling_port = module.storage.alloydb_connection_pooling_port
  alloydb_database_name           = module.storage.alloydb_database_name
  ingestion_staging_bucket_name   = var.ingestion_staging_bucket_name
  topic_continuous_audio_id       = module.message_queues.topic_continuous_audio_id
  topic_continuous_audio_name     = module.message_queues.topic_continuous_audio_name
  topic_segmented_audio_id        = module.message_queues.topic_segmented_audio_id
  topic_segmented_audio_name      = module.message_queues.topic_segmented_audio_name
  echo_recordings_bucket_name     = var.echo_recordings_bucket_name
  dev_recordings_bucket_name      = var.dev_recordings_bucket_name
  prod_project_id                 = var.prod_project_id
  network_name                    = module.network.network_name
  subnet_name                     = module.network.subnet_name
  worker_password_secret_id       = module.storage.worker_password_secret_id
  enable_monitoring               = var.enable_monitoring

  slack_critical_notification_channel_id = var.slack_critical_notification_channel_id
  fire_notifications_url_base            = var.fire_notifications_url_base
  fire_notifications_s3_base             = var.fire_notifications_s3_base
  fire_notifications_user                = var.fire_notifications_user
  fire_notifications_password            = var.fire_notifications_password
  echo_ingestion_max_instances           = var.echo_ingestion_max_instances

  depends_on = [
    google_project_service.apis,
    module.storage
  ]
}

module "segmentation" {
  source = "../services/pipeline/segmentation"

  environment                  = var.environment
  wif_service_account          = var.wif_service_account
  enable_monitoring            = var.enable_monitoring
  notification_channel_id      = var.slack_critical_notification_channel_id
  dataflow_staging_bucket_name = module.storage.dataflow_staging_bucket_name
}

module "normalization" {
  source = "../services/pipeline/normalization"

  region                          = var.region
  environment                     = var.environment
  canonical_audio_bucket_name     = module.storage.ingestion_canonical_bucket_name
  staging_audio_bucket_name       = module.storage.ingestion_staging_bucket_name
  topic_normalized_audio_id       = module.message_queues.topic_normalized_audio_id
  topic_segmented_audio_id        = module.message_queues.topic_segmented_audio_id
  topic_normalized_audio_dlq_name = module.message_queues.topic_normalized_audio_dlq_name
  audio_segments_api_url          = module.audio_segments_api.audio_segments_api_service_url
  audio_segments_service_name     = module.audio_segments_api.audio_segments_api_service_name
  max_instance_count              = var.normalization_max_instance_count
}

module "transcription" {
  source = "../services/pipeline/transcription"

  region                           = var.region
  environment                      = var.environment
  topic_normalized_audio_id        = module.message_queues.topic_normalized_audio_id
  topic_transcribed_audio_id       = module.message_queues.topic_transcribed_audio_id
  topic_transcribed_audio_dlq_name = module.message_queues.topic_transcribed_audio_dlq_name
  min_instances                    = var.transcription_min_instances
  max_instances                    = var.transcription_max_instances
  concurrency                      = var.transcription_concurrency
  enable_monitoring                = var.enable_monitoring

  transcriber_type            = var.transcriber_type
  transcriber_config          = var.transcriber_config
  audio_segments_service_name = module.audio_segments_api.audio_segments_api_service_name
  audio_segments_api_url      = module.audio_segments_api.audio_segments_api_service_url
  notification_channel_id     = var.slack_critical_notification_channel_id

  depends_on = [google_project_service.apis]
}

module "evaluation" {
  source = "../services/pipeline/evaluation"

  region                                 = var.region
  environment                            = var.environment
  rules_api_url                          = module.rules_api.rules_api_url
  topic_evaluated_audio_id               = module.message_queues.topic_evaluated_audio_id
  topic_transcribed_audio_id             = module.message_queues.topic_transcribed_audio_id
  rules_service_name                     = module.rules_api.rules_api_service_name
  audio_segments_service_name            = module.audio_segments_api.audio_segments_api_service_name
  topic_evaluated_audio_dead_letter_name = module.message_queues.topic_evaluated_audio_dead_letter_name
  audio_segments_api_url                 = module.audio_segments_api.audio_segments_api_service_url
  min_instances                          = var.evaluation_min_instances
  max_instances                          = var.evaluation_max_instances

  depends_on = [google_project_service.apis]
}

module "notification" {
  source = "../services/pipeline/notification"

  project_id                           = var.project_id
  project_number                       = data.google_project.project.number
  region                               = var.region
  environment                          = var.environment
  network_name                         = module.network.network_name
  subnet_name                          = module.network.subnet_name
  external_endpoint                    = var.external_endpoint
  external_endpoint_api_key            = var.external_endpoint_api_key
  redis_host                           = module.storage.redis_host
  redis_port                           = module.storage.redis_port
  redis_password_secret_id             = module.storage.redis_password_secret_id
  redis_password_secret_full_id        = module.storage.redis_password_secret_full_id
  redis_certificate_secret_id          = module.storage.redis_certificate_secret_id
  redis_certificate_secret_full_id     = module.storage.redis_certificate_secret_full_id
  topic_evaluated_audio_id             = module.message_queues.topic_evaluated_audio_id
  topic_evaluated_audio_dead_letter_id = module.message_queues.topic_evaluated_audio_dead_letter_id
  webapp_base_url                      = var.web_domain
  feeds_api_url                        = module.feeds_api.feeds_api_url
  rules_api_url                        = module.rules_api.rules_api_url

  depends_on = [
    google_project_service.apis,
    module.storage
  ]
}

# =============================================================================
# 6. Management API Services
# =============================================================================

module "rules_api" {
  source = "../services/api/rules"

  region                          = var.region
  environment                     = var.environment
  network_name                    = module.network.network_name
  subnet_name                     = module.network.subnet_name
  alloydb_instance_ip             = module.storage.alloydb_primary_instance_ip
  alloydb_connection_pooling_port = module.storage.alloydb_connection_pooling_port
  worker_password_secret_id       = module.storage.worker_password_secret_id
  radio_transcription_db_name     = module.storage.alloydb_database_name
}

module "audio_segments_api" {
  source = "../services/api/audio_segments"

  region                          = var.region
  environment                     = var.environment
  network_name                    = module.network.network_name
  subnet_name                     = module.network.subnet_name
  alloydb_instance_ip             = module.storage.alloydb_primary_instance_ip
  alloydb_connection_pooling_port = module.storage.alloydb_connection_pooling_port
  worker_password_secret_id       = module.storage.worker_password_secret_id
  radio_transcription_db_name     = module.storage.alloydb_database_name
}

module "feeds_api" {
  source = "../services/api/feeds"

  region                          = var.region
  environment                     = var.environment
  network_name                    = module.network.network_name
  subnet_name                     = module.network.subnet_name
  alloydb_instance_ip             = module.storage.alloydb_primary_instance_ip
  alloydb_connection_pooling_port = module.storage.alloydb_connection_pooling_port
  worker_password_secret_id       = module.storage.worker_password_secret_id
  radio_transcription_db_name     = module.storage.alloydb_database_name
}

module "radio_transcription_api" {
  source = "../services/api/radio-transcription-api"

  region                      = var.region
  environment                 = var.environment
  rules_api_url               = "${module.rules_api.rules_api_url}/v1/rules"
  feeds_api_url               = "${module.feeds_api.feeds_api_url}/v1/feeds"
  audio_segments_api_url      = "${module.audio_segments_api.audio_segments_api_service_url}/v1/audio_segments"
  google_auth_client_id       = var.google_auth_client_id
  google_auth_client_secret   = var.google_auth_client_secret
  allowed_origin              = var.web_domain
  api_public_url              = var.api_public_url
  workspace_admin_group_email = var.workspace_admin_group_email
}

# =============================================================================
# 7. INTERNAL TOOLING & ASSETS
# =============================================================================

# Artifact Registry for storing pipeline Docker images
resource "google_artifact_registry_repository" "services" {
  location      = var.region
  repository_id = "radio-transcription-services-${var.environment}"
  description   = "Docker repository for Cloud Run services"
  format        = "DOCKER"

  depends_on = [google_project_service.apis]
}

module "deployer" {
  count = local.deployer_iam_enabled ? 1 : 0

  source = "./deployer"

  deployer_sa_email            = local.deployer_sa_email
  dataflow_staging_bucket_name = module.storage.dataflow_staging_bucket_name
}


# =============================================================================
# 8. MONITORING & ALERTING
# =============================================================================

module "monitoring" {
  count = var.enable_monitoring ? 1 : 0

  source = "./monitoring"

  environment             = var.environment
  notification_channel_id = var.slack_critical_notification_channel_id
}

