# -----------------------------------------------------------------------------
# AlloyDB
# -----------------------------------------------------------------------------

output "alloydb_cluster_name" {
  description = "The AlloyDB cluster name."
  value       = module.alloydb.cluster_name
}

output "alloydb_primary_instance_ip" {
  description = "The IP address of the primary AlloyDB instance."
  value       = module.alloydb.primary_instance_ip
}

output "alloydb_connection_pooling_port" {
  description = "The port used for connection pooling."
  value       = module.alloydb.connection_pooling_port
}

output "alloydb_worker_password" {
  description = "The generated password for the AlloyDB worker user."
  value       = random_password.alloydb_worker.result
  sensitive   = true
}

output "postgres_password_secret_id" {
  description = "The Secret Manager secret ID for the postgres password."
  value       = google_secret_manager_secret.alloydb_postgres_password.secret_id
}

output "worker_password_secret_id" {
  description = "The Secret Manager secret ID for the worker password."
  value       = google_secret_manager_secret.alloydb_worker_password.secret_id
}

output "alloydb_database_name" {
  description = "The name of the database being used by AlloyDB (likely 'postgres')."
  value       = module.alloydb.database_name
}


# -----------------------------------------------------------------------------
# Redis
# -----------------------------------------------------------------------------

output "redis_host" {
  description = "The host IP of the Redis instance."
  value       = module.notification_pipeline_redis.host
}

output "redis_port" {
  description = "The port of the Redis instance."
  value       = module.notification_pipeline_redis.port
}

output "redis_password_secret_id" {
  description = "The Secret Manager secret ID for the Redis password."
  value       = google_secret_manager_secret.notification_pipeline_redis_password.secret_id
}

output "redis_password_secret_full_id" {
  description = "The full Secret Manager resource ID for the Redis password (used for IAM bindings)."
  value       = google_secret_manager_secret.notification_pipeline_redis_password.id
}

output "redis_certificate_secret_id" {
  description = "The Secret Manager secret ID for the Redis certificate."
  value       = google_secret_manager_secret.notification_pipeline_redis_certificate.secret_id
}

output "redis_certificate_secret_full_id" {
  description = "The full Secret Manager resource ID for the Redis certificate (used for IAM bindings)."
  value       = google_secret_manager_secret.notification_pipeline_redis_certificate.id
}

# -----------------------------------------------------------------------------
# GCS Buckets
# -----------------------------------------------------------------------------

output "dataflow_staging_bucket_name" {
  description = "The name of the Dataflow staging bucket."
  value       = module.dataflow_staging_bucket.name
}

output "ingestion_staging_bucket_name" {
  description = "The name of the ingestion staging bucket."
  value       = module.ingestion_staging_bucket.name
}

output "ingestion_canonical_bucket_name" {
  description = "The name of the ingestion canonical bucket."
  value       = module.ingestion_canonical_bucket.name
}
