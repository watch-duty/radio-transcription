# =============================================================================
# INGESTION PIPELINE
# =============================================================================

data "google_project" "project" {}

locals {
  project_id              = data.google_project.project.project_id
  project_number          = data.google_project.project.number
  otel_traces_sampler     = var.environment == "dev" ? "parentbased_traceidratio" : "parentbased_always_on"
  otel_traces_sampler_arg = var.environment == "dev" ? "0.05" : "1.0"
  otel_bsp_max_batch_size = var.environment == "dev" ? "512" : "64"
  otel_bsp_schedule_delay = var.environment == "dev" ? "5000" : "1000"
}

module "collector_mig" {
  source = "../../../container_mig"

  project_id            = local.project_id
  region                = var.region
  name_prefix           = "ingestion-collector-${var.environment}"
  subnetwork_id         = var.subnet_id
  service_account_email = google_service_account.audio_ingestion_worker.email
  container_image       = "${var.region}-docker.pkg.dev/${local.project_id}/radio-transcription-services-${var.environment}/ingestion:latest"
  cos_image_self_link   = var.cos_image_self_link
  available_zones       = var.available_zones

  container_env = {
    BROADCASTIFY_USERNAME          = var.broadcastify_username
    BROADCASTIFY_PASSWORD          = var.broadcastify_password
    BROADCASTIFY_XAN_TOKEN         = var.broadcastify_xan_token
    BROADCASTIFY_JWT_SECRET_ID     = "broadcastify-jwt-${var.environment}"
    BROADCASTIFY_API_APP_ID        = var.broadcastify_api_app_id
    BROADCASTIFY_API_KEY           = var.broadcastify_api_key
    BROADCASTIFY_API_KEY_ID        = var.broadcastify_api_key_id
    ALLOYDB_HOST                   = var.alloydb_primary_instance_ip
    ALLOYDB_USER                   = "worker"
    ALLOYDB_DB                     = var.alloydb_database_name
    ALLOYDB_PASSWORD               = var.alloydb_worker_password
    ALLOYDB_PORT                   = tostring(var.alloydb_connection_pooling_port)
    AUDIO_STAGING_BUCKET           = var.ingestion_staging_bucket_name
    CONTINUOUS_PUBSUB_TOPIC_PATH   = var.topic_continuous_audio_id
    SEGMENTED_PUBSUB_TOPIC_PATH    = var.topic_segmented_audio_id
    GOOGLE_CLOUD_PROJECT           = local.project_id
    MAX_FEEDS_PER_WORKER           = "800"
    IS_GCP                         = "true"
    IS_INGESTION_SERVICE           = "true"
    FEED_AUDIT_ACTOR_ID            = "service_account:gcp:${google_service_account.audio_ingestion_worker.email}"
    FIRE_NOTIFICATIONS_URL_BASE    = var.fire_notifications_url_base
    FIRE_NOTIFICATIONS_S3_BASE     = var.fire_notifications_s3_base
    FIRE_NOTIFICATIONS_USER        = var.fire_notifications_user
    FIRE_NOTIFICATIONS_PASSWORD    = var.fire_notifications_password
    ICECAST_SEGMENT_DIR            = "/tmp/icecast_segments"
    OTEL_TRACES_SAMPLER            = local.otel_traces_sampler
    OTEL_TRACES_SAMPLER_ARG        = local.otel_traces_sampler_arg
    OTEL_BSP_MAX_EXPORT_BATCH_SIZE = local.otel_bsp_max_batch_size
    OTEL_BSP_SCHEDULE_DELAY        = local.otel_bsp_schedule_delay
  }

  tmpfs_mounts = ["/tmp/icecast_segments:rw,noexec,nosuid,size=4g"]

  machine_type = var.collector_machine_type
  target_size  = 2

  # BALANCED distribution flipped from ANY in Phase 2 (PR for MIG-07/08/09).
  # Rationale: Phase 2 raises target_size 1→2, so a regional MIG should spread
  # VMs across multiple zones to survive a single-zone outage without operator
  # action. ANY was the right choice at target_size=1 (one VM, anywhere is
  # fine); at target_size≥2 we want multi-zone placement.
  #
  # BALANCED, not EVEN: the container_mig module also configures an instance
  # flexibility policy (the ranked-machine-type fallback below — n2 → n2d →
  # c3 → e2). GCE rejects EVEN with any flexibility policy at apply time:
  #   Error 400: Invalid value for field 'resource.distributionPolicy.targetShape':
  #   'EVEN'. Instance flexibility policy is supported only for RMIGs with
  #   target shapes ANY, ANY_SINGLE_ZONE and BALANCED.
  # BALANCED is the closest semantically — best-effort balance across zones,
  # but allows skew when capacity is constrained, which pairs naturally with
  # the machine-type fallback (we already accept "any compatible capacity is
  # better than waiting"). EVEN's hard guarantee would force the MIG to fail
  # rather than skew, which contradicts the flexibility-policy intent.
  #
  # Phase 3 attaches the autoscaler which raises target_size to a 2-10 range
  # — BALANCED scales correctly across that range.
  distribution_policy_target_shape = "BALANCED"


  # Attach a google_compute_health_check probing /healthz on port 8080 and
  # wire it into the MIG's auto_healing_policies. VMs that fail 3 consecutive
  # probes after the 5-min initial_delay window are replaced. Requires the
  # allow_health_checks firewall below.
  enable_autohealing = true

  labels = {
    environment = var.environment
    service     = "radio-transcription"
    component   = "ingestion-collector"
  }

  depends_on = [
    google_project_iam_member.collector_ar_reader,
    google_pubsub_topic_iam_member.collector_publisher,
    google_storage_bucket_iam_member.audio_ingestion_bucket_writer,
    google_secret_manager_secret_iam_member.collector_broadcastify_jwt_accessor,
    google_compute_firewall.allow_health_checks,
  ]
}

# =============================================================================
# ECHO AUDIO INGESTION
# =============================================================================

module "echo" {
  source = "./echo"

  region                          = var.region
  environment                     = var.environment
  echo_recordings_bucket_name     = var.echo_recordings_bucket_name
  dev_recordings_bucket_name      = var.dev_recordings_bucket_name
  prod_project_id                 = var.prod_project_id
  network_name                    = var.network_name
  subnet_name                     = var.subnet_name
  alloydb_primary_instance_ip     = var.alloydb_primary_instance_ip
  alloydb_connection_pooling_port = var.alloydb_connection_pooling_port
  alloydb_database_name           = var.alloydb_database_name
  ingestion_staging_bucket_name   = var.ingestion_staging_bucket_name
  topic_segmented_audio_id        = var.topic_segmented_audio_id
  topic_segmented_audio_name      = var.topic_segmented_audio_name
  worker_password_secret_id       = var.worker_password_secret_id
  echo_ingestion_max_instances   = var.echo_ingestion_max_instances
}

# =============================================================================
# BROADCASTIFY CREDENTIAL ROTATION
# =============================================================================

module "broadcastify_credential_rotation" {
  source = "./broadcastify_credential_rotation"

  project_id              = local.project_id
  region                  = var.region
  environment             = var.environment
  project_number          = local.project_number
  broadcastify_api_key    = var.broadcastify_api_key
  broadcastify_api_key_id = var.broadcastify_api_key_id
  broadcastify_api_app_id = var.broadcastify_api_app_id
  broadcastify_username   = var.broadcastify_username
  broadcastify_password   = var.broadcastify_password
  notification_channel_id = var.slack_critical_notification_channel_id
}

# =============================================================================
# OLDEST-FEED PUBLISHER (Cloud Run v2 + Cloud Scheduler — Phase 2 PUB-01)
# =============================================================================
# Publishes custom.googleapis.com/feeds/unclaimed_count every 60s; consumed
# by the Phase 3 autoscaler as the primary scaling signal. The metric
# descriptor lives in this file (next to quarantine_events) per METRIC-01.
#
# NOTE on naming: the directory and Cloud Run service name still say
# "oldest-feed-publisher" — semantic mismatch with the new metric. Renaming
# would force destroy + recreate of the Cloud Run service, scheduler, and
# IAM bindings. A follow-up PR will likely add the latency metric back as a
# second write from this same service (driving an SLO alert), at which point
# either name fits or rename becomes a deliberate cleanup.

module "oldest_feed_publisher" {
  source = "./oldest_feed_publisher"

  project_id                      = local.project_id
  region                          = var.region
  environment                     = var.environment
  project_number                  = local.project_number
  network_name                    = var.network_name
  subnet_name                     = var.subnet_name
  alloydb_primary_instance_ip     = var.alloydb_primary_instance_ip
  alloydb_connection_pooling_port = var.alloydb_connection_pooling_port
  alloydb_database_name           = var.alloydb_database_name
  worker_password_secret_id       = var.worker_password_secret_id

  notification_channel_id = var.slack_critical_notification_channel_id
}

# Two-signal MAX policy on TWO ADDITIVE metrics. The autoscaler scales on
# whichever signal demands more capacity; both must be under target for
# scale-in.
#
# PRIMARY signal: queue length (custom.googleapis.com/feeds/unclaimed_count,
# published by oldest-feed-publisher-${env} every 60s). Per-group additive
# metric — single_instance_assignment math is the documented pattern for this
# shape. Each VM is responsible for absorbing N unclaimed feeds; autoscaler
# scales to keep total_queue <= N * VMs. Queue length is a LEADING indicator,
# growing the moment claim rate falls behind arrival rate — well before any
# individual feed has been waiting long enough to breach the 60s SLO.
#
# BACKSTOP signal: CPU 75%. Proportional per-VM math (well-defined GCP
# semantics). Catches CPU-bound cases the queue-length signal misses — e.g.,
# a flood of high-bitrate feeds claimed quickly (so queue stays low) but
# processed slowly (so individual workers saturate CPU).
#
# What's NOT used as an autoscaling signal: oldest_unclaimed_age_seconds
# (latency). Earlier iterations of this milestone tried using latency as the
# scaling signal; cross-AI review on 2026-04-29 revealed it's mathematically
# unsound for GCP autoscaling. Latency is non-additive (adding a VM doesn't
# proportionally reduce the value) and per-group (no instance labels), and
# capacity-based math `desired = ceil(latency / target)` tolerates SLO
# breaches up to 60s × current_replicas before reacting. The latency metric
# now lives as an SLO observability signal that drives an alert policy in a
# follow-up PR.
#
# What's HONESTLY out of scope for this autoscaler: per-type cap saturation,
# AlloyDB latency, network bottlenecks. When `unclaimed_count` plateaus high
# while VMs are idle, that's a non-VM-count bottleneck — adding more VMs
# WON'T HELP. The follow-up alert policy will catch this case (sustained
# latency breach with no autoscaler reaction); operator runbook will route
# to the appropriate workstream owner (worker-hardening, DB tuning, etc.).
#
# The MIG manager output `instance_group_manager_id` already exists in the
# container_mig module (SCALE-08 satisfied without a module change). The MIG
# resource itself already has `lifecycle.ignore_changes = [target_size]`
# (added in radio-transcription PR #237 commit a20a510e, well before this
# milestone), so terraform plan stays clean once the autoscaler takes over
# target_size.
resource "google_compute_region_autoscaler" "collector" {
  name    = "ingestion-collector-${var.environment}-asg"
  project = local.project_id
  region  = var.region
  target  = module.collector_mig.instance_group_manager_id

  autoscaling_policy {
    min_replicas    = 2
    max_replicas    = 10
    cooldown_period = 60

    # PRIMARY scaling signal. Each VM absorbs `single_instance_assignment`
    # unclaimed feeds at the threshold; autoscaler scales to keep total queue
    # count <= N * VMs.
    #
    # Sizing N=100: workers run a 5s claim cycle with up to 800 feeds per
    # worker (k=2 workers/VM = 1,600 feeds/cycle/VM in burst). N=100 is
    # deliberately conservative — well below per-VM burst capacity — so the
    # autoscaler reacts to backlog before it overwhelms a single VM's
    # drain. A 100-feed backlog drains in ~1 cycle (~5s) so the SLO holds.
    # The conservative N also leaves headroom for the per-type caps
    # (bcfy_feeds=240, bcfy_calls=600, openmhz=900) which bound a single
    # worker's feed mix below the raw 800 limit. Tunable from production
    # data.
    #
    # Concrete behavior at N=100:
    #   unclaimed_count = 100  → desired = ceil(100/100) = 1   (clamped to min=2)
    #   unclaimed_count = 250  → desired = ceil(250/100) = 3
    #   unclaimed_count = 500  → desired = ceil(500/100) = 5
    #   unclaimed_count = 1000 → desired = ceil(1000/100) = 10 (max_replicas)
    metric {
      name                       = "custom.googleapis.com/feeds/unclaimed_count"
      single_instance_assignment = 100
      # `type` is intentionally omitted. Per the terraform google provider
      # docs, `type` "Conflicts with: single_instance_assignment" — and GCE
      # enforces this at apply time:
      #   Error: "utilization_target_type" can't be set when
      #   "single_instance_assignment" is used.
      # `type` is only meaningful when paired with `target` (utilization-
      # based math); single_instance_assignment uses capacity-based math
      # and the metric kind comes from the metric descriptor itself.

      # Defense in depth: scope to series with `resource.type = "global"` so
      # the autoscaler doesn't silently aggregate any future per-VM emission
      # of the same metric type. Today the Publisher is the only writer and
      # only writes resource.type=global, so this is a no-op; tomorrow if
      # someone adds a second writer (e.g. per-VM debug emission), the
      # autoscaler still scales on just the global signal.
      filter = "resource.type = \"global\""
    }

    # BACKSTOP scaling signal. Proportional per-VM math; catches CPU-bound
    # cases where queue-length wouldn't trigger (claims drain into 'leased'
    # state quickly but workers can't keep up with per-feed processing).
    cpu_utilization {
      target = 0.75
    }

    # At most 1 VM removed per 5-minute window. Protection against the worker's
    # 90s graceful-shutdown window (PR #359 SHUTDOWN-01) being overwhelmed by
    # aggressive scale-in. Scale-out is uncapped (cooldown_period=60 still
    # throttles it to one decision per minute, which is already the
    # autoscaler's natural cadence).
    scale_in_control {
      max_scaled_in_replicas {
        fixed = 1
      }
      time_window_sec = 300
    }
  }
}


# =============================================================================
# MONITORING (LOG-BASED METRICS + SLOS)
# =============================================================================

module "monitoring" {
  count  = var.enable_monitoring ? 1 : 0
  source = "./monitoring"

  region                  = var.region
  environment             = var.environment
  notification_channel_id = var.slack_critical_notification_channel_id
}


# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

# Service account for the audio ingestion pipeline
resource "google_service_account" "audio_ingestion_worker" {
  account_id   = "audio-ingestion-pipeline-${var.environment}"
  display_name = "Audio Ingestion Pipeline Service Account"
  description  = "Service account for the audio ingestion pipeline to run"
}


# Allow audio ingestion pipeline to write files to the staging bucket
resource "google_storage_bucket_iam_member" "audio_ingestion_bucket_writer" {
  bucket = var.ingestion_staging_bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}

# Allow SSH via Identity-Aware Proxy for debugging MIG instances.
# Priority 100 is load-bearing: it must be higher priority (lower number) than the
# deny_all_ingress rule at 500 below, otherwise the deny would swallow IAP SSH too.
resource "google_compute_firewall" "iap_ssh" {
  name     = "allow-iap-ssh-${var.environment}"
  project  = local.project_id
  network  = var.network_id
  priority = 100

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges           = ["35.235.240.0/20"]
  target_service_accounts = [google_service_account.audio_ingestion_worker.email]
}

# Allow GCP health-check probers to reach /healthz on port 8080. Priority 200
# is load-bearing: it must be higher priority (lower number) than
# deny_all_ingress at 500, otherwise the deny would swallow probes and every
# VM would be flagged unhealthy → autohealer would recreate forever. Source
# ranges are the documented GCP prober CIDRs (not the full internet), so
# port 8080 is NOT exposed to arbitrary hosts — only to Google's probers.
# See https://cloud.google.com/load-balancing/docs/health-check-concepts#ip-ranges
resource "google_compute_firewall" "allow_health_checks" {
  name     = "allow-health-checks-${var.environment}"
  project  = local.project_id
  network  = var.network_id
  priority = 200

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }

  source_ranges           = ["130.211.0.0/22", "35.191.0.0/16"]
  target_service_accounts = [google_service_account.audio_ingestion_worker.email]
}

# Defense-in-depth: explicit deny for all other ingress to the audio ingestion
# VMs. Because these instances now have public IPs (Cloud NAT was deleted in the
# same PR), relying solely on GCP's implied deny is brittle — a future overly
# broad allow rule (allow-all-internal, broad target tags) would silently expose
# them. This rule, targeting the service account, makes the dark-by-default
# posture explicit and independent of the VPC's other firewall configuration.
resource "google_compute_firewall" "deny_all_ingress" {
  name      = "deny-all-ingress-audio-ingestion-${var.environment}"
  project   = local.project_id
  network   = var.network_id
  direction = "INGRESS"
  priority  = 500

  deny {
    protocol = "all"
  }

  source_ranges           = ["0.0.0.0/0"]
  target_service_accounts = [google_service_account.audio_ingestion_worker.email]
}


# Pull Docker images from Artifact Registry
resource "google_project_iam_member" "collector_ar_reader" {
  project = local.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}


# Write logs to Cloud Logging
resource "google_project_iam_member" "collector_log_writer" {
  project = local.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}


# Write metrics to Cloud Monitoring
resource "google_project_iam_member" "collector_metric_writer" {
  project = local.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}

# IAM-08: Add roles/alloydb.client to the existing worker SA for parity
# with the Publisher (which gets the same role). Worker today connects
# via PgBouncer + password and functions without this role; the role is
# required for AlloyDB IAM auth tooling and for operational `gcloud
# alloydb instances ...` commands run as this SA. Adding it project-wide
# has negligible blast radius (the SA already has metric-writer +
# log-writer + trace-writer at the project scope).
resource "google_project_iam_member" "collector_alloydb_client" {
  project = local.project_id
  role    = "roles/alloydb.client"
  member  = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}

# Write traces to Cloud Trace
resource "google_project_iam_member" "collector_trace_writer" {
  project = local.project_id
  role    = "roles/cloudtrace.agent"
  member  = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}


# Publish audio storage trigger messages to continuous-audio topic
resource "google_pubsub_topic_iam_member" "collector_publisher" {
  project = local.project_id
  topic   = var.topic_continuous_audio_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}

# Publish audio storage trigger messages to segmented-audio topic
resource "google_pubsub_topic_iam_member" "collector_publisher_segmented" {
  project = local.project_id
  topic   = var.topic_segmented_audio_name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
}

# Read Broadcastify JWT token from Secret Manager (rotated by credential rotation service)
resource "google_secret_manager_secret_iam_member" "collector_broadcastify_jwt_accessor" {
  project    = local.project_id
  secret_id  = "broadcastify-jwt-${var.environment}"
  role       = "roles/secretmanager.secretAccessor"
  member     = "serviceAccount:${google_service_account.audio_ingestion_worker.email}"
  depends_on = [module.broadcastify_credential_rotation]
}
