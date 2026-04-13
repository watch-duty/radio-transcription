# =============================================================================
# CONTAINER MIG MODULE
# =============================================================================
# Deploys a Docker container on a GCE Managed Instance Group using
# Container-Optimized OS (COS) with cloud-init for container orchestration.
#
# The module is container-agnostic: any Docker image from Artifact Registry
# can be deployed by setting container_image and container_env.
# =============================================================================

locals {
  registry_host = split("/", var.container_image)[0]

  # Docker --env-file format: KEY=VALUE per line, no quoting needed.
  # Values are taken literally — safe for special chars like $ ' " \
  # Each line is pre-indented with 4 spaces to satisfy the YAML block
  # scalar in cloud_config.yaml.tftpl (content: |).
  env_file_content = join("\n", [
    for k, v in var.container_env : "    ${k}=${v}"
  ])
}

# Fetch the latest stable Container-Optimized OS image
data "google_compute_image" "cos" {
  family  = "cos-stable"
  project = "cos-cloud"
}

# Zones in the region. Used to size update_policy.max_surge_fixed on the
# regional MIG below; GCP requires it to be >= the number of zones the MIG
# operates in. Computing dynamically means the module works in regions with
# more or fewer zones (e.g. us-central1 has 4, us-east5 has 3) without a
# per-region hardcoded value.
data "google_compute_zones" "available" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# Instance Template
# -----------------------------------------------------------------------------

resource "google_compute_instance_template" "this" {
  # name_prefix generates unique names (e.g. "icecast-collector-prod-abc123")
  # required for create_before_destroy lifecycle
  name_prefix  = "${var.name_prefix}-"
  project      = var.project_id
  region       = var.region
  machine_type = var.machine_type

  disk {
    source_image = data.google_compute_image.cos.self_link
    disk_size_gb = var.boot_disk_size_gb
    auto_delete  = true
    boot         = true
    disk_type    = "pd-standard"
  }

  network_interface {
    subnetwork = var.subnetwork_id
    access_config {
      network_tier = "PREMIUM"
    }
  }

  service_account {
    email  = var.service_account_email
    scopes = ["cloud-platform"]
  }

  metadata = {
    google-logging-enabled = "true"
    user-data = templatefile("${path.module}/cloud_config.yaml.tftpl", {
      service_name     = var.name_prefix
      registry_host    = local.registry_host
      container_image  = var.container_image
      env_file_content = local.env_file_content
    })
  }

  labels = merge(var.labels, {
    managed_by = "terraform"
  })

  lifecycle {
    create_before_destroy = true
  }
}

# -----------------------------------------------------------------------------
# Managed Instance Group
# -----------------------------------------------------------------------------

resource "google_compute_region_instance_group_manager" "this" {
  name               = "${var.name_prefix}-mig"
  project            = var.project_id
  region             = var.region
  base_instance_name = var.name_prefix
  target_size        = var.target_size

  version {
    instance_template = google_compute_instance_template.this.self_link_unique
  }

  # Regional MIG distributes across all zones in the region by default. When one
  # zone lacks capacity (ZONE_RESOURCE_POOL_EXHAUSTED), the MIG automatically
  # places instances in other zones — which is what the zonal predecessor
  # couldn't do, and what wedged dev after the NAT-removal apply.
  update_policy {
    type                         = "PROACTIVE"
    minimal_action               = "REPLACE"
    max_surge_fixed              = length(data.google_compute_zones.available.names)
    max_unavailable_fixed        = 0
    replacement_method           = "SUBSTITUTE"
    instance_redistribution_type = "PROACTIVE"
  }
}
