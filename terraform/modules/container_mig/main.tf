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
  registry_host  = split("/", var.container_image)[0]
  vm_health_port = 8080
  # Current container-MIG topology runs two worker containers per VM. Each
  # worker exposes container /healthz on host port vm_health_port + index, so
  # worker 1 is :8081 and worker 2 is :8082. The VM Health agent itself
  # listens on vm_health_port (:8080). Keep this as an internal local until a
  # deployment needs a different per-VM worker count.
  worker_indices = [1, 2]
  vm_health_worker_endpoints = join(",", [
    for worker_index in local.worker_indices :
    "http://127.0.0.1:${local.vm_health_port + worker_index}/healthz"
  ])
  worker_systemd_after_units = join(" ", [
    for worker_index in local.worker_indices :
    "${var.name_prefix}@${worker_index}.service"
  ])

  # Docker --env-file format: KEY=VALUE per line, no quoting needed.
  # Values are taken literally — safe for special chars like $ ' " \
  # Each line is pre-indented with 4 spaces to satisfy the YAML block
  # scalar in cloud_config.yaml.tftpl (content: |).
  env_file_content = join("\n", [
    for k, v in var.container_env : "    ${k}=${v}"
  ])

  cos_image_self_link = var.cos_image_self_link != null ? var.cos_image_self_link : data.google_compute_image.cos[0].self_link
  available_zones     = var.available_zones != null ? var.available_zones : data.google_compute_zones.available[0].names
  zone_count          = length(local.available_zones)
}

# Fetch the latest stable Container-Optimized OS image
data "google_compute_image" "cos" {
  count   = var.cos_image_self_link == null ? 1 : 0
  family  = "cos-stable"
  project = "cos-cloud"
}

# Zones in the region. Used to size update_policy.max_surge_fixed on the
# regional MIG below; GCP requires it to be >= the number of zones the MIG
# operates in. Computing dynamically means the module works in regions with
# more or fewer zones (e.g. us-central1 has 4, us-east5 has 3) without a
# per-region hardcoded value.
data "google_compute_zones" "available" {
  count   = var.available_zones == null ? 1 : 0
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# Instance Template
# -----------------------------------------------------------------------------

resource "google_compute_instance_template" "this" {
  # name_prefix generates unique names (e.g. "ingestion-collector-prod-abc123")
  # required for create_before_destroy lifecycle
  name_prefix  = "${var.name_prefix}-"
  project      = var.project_id
  region       = var.region
  machine_type = var.machine_type

  disk {
    source_image = local.cos_image_self_link
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
      service_name               = var.name_prefix
      registry_host              = local.registry_host
      container_image            = var.container_image
      env_file_content           = local.env_file_content
      enable_autohealing         = var.enable_autohealing
      tmpfs_mounts               = var.tmpfs_mounts
      vm_health_port             = local.vm_health_port
      worker_indices             = local.worker_indices
      vm_health_worker_endpoints = local.vm_health_worker_endpoints
      worker_systemd_after_units = local.worker_systemd_after_units
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
# Health check (opt-in via var.enable_autohealing)
# -----------------------------------------------------------------------------

# HTTP probe of /healthz on port 8080. Attached to auto_healing_policies on
# the MIG below; VMs that fail 3 consecutive probes (90s) are replaced.
# count-gated so callers that don't opt in don't pay for an unused resource.
resource "google_compute_health_check" "this" {
  count = var.enable_autohealing ? 1 : 0

  name                = "${var.name_prefix}-healthz"
  project             = var.project_id
  check_interval_sec  = 30
  timeout_sec         = 10
  healthy_threshold   = 1
  unhealthy_threshold = 3 # 3 consecutive failures = 90s minimum; up to 120s from problem onset

  http_health_check {
    port         = local.vm_health_port
    request_path = "/healthz"
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

  # Block `terraform apply` on MIG stabilization (up to the default 15-min
  # resource timeout). Without this, terraform considers the resource "applied"
  # once the MIG manager config is set, which is before the rolling update
  # actually places a VM. If the rolling update fails (zone exhaustion, image
  # pull failure, etc.), terraform silently returns green. With this, the apply
  # fails loudly — the right signal for operators.
  #
  # wait_for_instances_status stays at its default (STABLE). The provider's
  # StringInSlice validator only accepts "STABLE" and "UPDATED" — there is no
  # "HEALTHY" option despite what looked intuitive. "UPDATED" is unusable here
  # because it's incompatible with OPPORTUNISTIC update_policy: OPPORTUNISTIC
  # never cycles VMs onto a new template, so "wait for all VMs on latest
  # template" would block apply until the 30m resource timeout. The autohealer's
  # initial_delay_sec (auto_healing_policies block below) is the real health
  # gate; terraform gates only on "VMs are RUNNING."
  wait_for_instances = true

  version {
    instance_template = google_compute_instance_template.this.self_link_unique
  }

  # Keep normal repair as the default while allowing callers to represent a
  # fail-stopped maintenance boundary. Application autohealing remains an
  # independent opt-in below because both repair paths must be disabled during
  # an exclusive authority transition.
  instance_lifecycle_policy {
    default_action_on_failure = var.default_action_on_vm_failure
  }

  # Autohealing. initial_delay_sec = 300 (5 min) covers VM boot (~30s) +
  # cloud-init (~60s) + docker pull (~2-3 min on cold cache) + container
  # start + first heartbeat + first feed lease. A VM failing /healthz during
  # this window would be killed mid-startup, guaranteeing a recreate loop on
  # any non-trivial rollout — so the Python /healthz handler also returns 200
  # during its own startup grace (health_check_startup_grace_sec, default
  # 120s) to keep the signal consistent with what the autohealer expects.
  dynamic "auto_healing_policies" {
    for_each = var.enable_autohealing ? [1] : []
    content {
      health_check      = google_compute_health_check.this[0].id
      initial_delay_sec = 300
    }
  }

  # target_shape: ANY is right for small MIGs (target_size=1, as in dev)
  # because EVEN anchors a single VM to one zone and won't fall back on
  # capacity exhaustion (the 2026-04-13 dev incident). EVEN/BALANCED is
  # right at scale (prod 25-120 VMs) because ANY can concentrate VMs in a
  # single zone, turning one zone's outage into a fleet-wide outage.
  # Caller-specified so each deployment can pick the appropriate default.
  distribution_policy_target_shape = var.distribution_policy_target_shape

  # Machine-type fallback: when a given machine class is exhausted across all
  # zones, the MIG walks the instance_selections ranks in order until one
  # succeeds. Overrides the instance template's machine_type at VM creation
  # time. Callers that don't need this (single machine type, willing to fail)
  # can leave var.instance_selections empty.
  dynamic "instance_flexibility_policy" {
    for_each = length(var.instance_selections) > 0 ? [1] : []
    content {
      dynamic "instance_selections" {
        for_each = var.instance_selections
        content {
          name          = instance_selections.value.name
          rank          = instance_selections.value.rank
          machine_types = instance_selections.value.machine_types
        }
      }
    }
  }

  # Regional MIG distributes across all zones in the region by default. When one
  # zone lacks capacity (ZONE_RESOURCE_POOL_EXHAUSTED), the MIG automatically
  # places instances in other zones — which is what the zonal predecessor
  # couldn't do, and what wedged dev after the NAT-removal apply.
  #
  # update_policy.type is tied to instance_flexibility_policy presence:
  #   - No flex policy: PROACTIVE (MIG actively rolls VMs onto the latest
  #     template on every template-version change — standard behavior).
  #   - With flex policy: OPPORTUNISTIC (required by the GCP API — PROACTIVE
  #     + flex is rejected as "Instance flexibility policy requires
  #     opportunistic update policy"). VMs update only when replaced for
  #     another reason (autohealing, manual rolling-action replace, or
  #     autoscaler). Explicit rollouts can be triggered via
  #     `gcloud compute instance-groups managed rolling-action replace`.
  #
  # instance_redistribution_type is tied to target_shape:
  #   - EVEN: PROACTIVE (converge toward even zonal distribution)
  #   - BALANCED / ANY / ANY_SINGLE_ZONE: NONE — there's no "even" target to
  #     converge toward, and GCP's API can reject PROACTIVE with these shapes.
  update_policy {
    type                         = length(var.instance_selections) > 0 ? "OPPORTUNISTIC" : "PROACTIVE"
    minimal_action               = "REPLACE"
    max_surge_fixed              = local.zone_count
    max_unavailable_fixed        = 0
    replacement_method           = "SUBSTITUTE"
    instance_redistribution_type = var.distribution_policy_target_shape == "EVEN" ? "PROACTIVE" : "NONE"
  }

  # Resource-level timeout. Default is 15 min per Terraform google provider,
  # which is tight when wait_for_instances=true must wait for a rolling update
  # across many VMs — especially if instance_flexibility_policy has to walk
  # down ranks after capacity errors (each failed attempt + backoff adds time).
  # 30 min covers prod-scale rollouts (25-120 VMs) with fallback; small MIGs
  # finish well inside this.
  timeouts {
    create = "30m"
    update = "30m"
  }

  # Ignore drift on target_size once the MIG exists. Rationale:
  # - Emergency manual scaling via gcloud (e.g., incident response) shouldn't
  #   be clobbered by the next unrelated `terraform apply`.
  # - When an autoscaler is added later, it will own target_size — terraform
  #   sets the initial value, autoscaler takes it from there.
  # Trade-off: changing var.target_size via terraform no longer has effect
  # after initial creation; scale via gcloud, autoscaler, or taint+recreate.
  lifecycle {
    ignore_changes = [target_size]
  }
}
