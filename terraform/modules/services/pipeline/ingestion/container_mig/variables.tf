# -----------------------------------------------------------------------------
# Required Variables
# -----------------------------------------------------------------------------

variable "project_id" {
  description = "The GCP project ID."
  type        = string
}

variable "region" {
  description = "The GCP region. The MIG is regional and distributes instances across all zones in this region; individual zone placement is handled by GCP."
  type        = string
}

variable "name_prefix" {
  description = "Name prefix for all resources (instance template, MIG, systemd service). Must be 1-48 characters."
  type        = string

  validation {
    condition     = can(regex("^[a-z][-a-z0-9]{0,47}$", var.name_prefix))
    error_message = "name_prefix must be 1-48 chars, start with a lowercase letter, and contain only lowercase letters, digits, and hyphens."
  }
}

variable "subnetwork_id" {
  description = "The self-link of the subnet where instances are placed."
  type        = string
}

variable "service_account_email" {
  description = "The service account email for the GCE instances."
  type        = string
}

variable "container_image" {
  description = "Full Artifact Registry image path (e.g. us-central1-docker.pkg.dev/project/repo/image:tag)."
  type        = string
}

# -----------------------------------------------------------------------------
# Optional Variables
# -----------------------------------------------------------------------------

variable "container_env" {
  description = "Environment variables passed to the container via docker run -e flags."
  type        = map(string)
  default     = {}
  sensitive   = true
}

variable "machine_type" {
  description = "The GCE machine type for each instance."
  type        = string
  default     = "e2-small"
}

variable "target_size" {
  description = "Fixed number of instances in the MIG."
  type        = number
  default     = 1

  validation {
    condition     = var.target_size >= 0
    error_message = "target_size must be non-negative."
  }
}

variable "boot_disk_size_gb" {
  description = "Boot disk size in GB."
  type        = number
  default     = 20
}

variable "labels" {
  description = "Labels to apply to GCE resources."
  type        = map(string)
  default     = {}
}

variable "instance_selections" {
  description = "Ranked fallback list of machine types for the regional MIG's instance_flexibility_policy. Lowest rank is tried first; subsequent ranks are fallbacks when GCP returns capacity errors on the preferred machine type. Leave empty to use only the machine_type on the instance template (no flexibility)."
  type = list(object({
    name          = string
    rank          = number
    machine_types = list(string)
  }))
  default = []
}

variable "distribution_policy_target_shape" {
  description = "How the regional MIG distributes VMs across zones. EVEN (default) distributes as evenly as possible — right for multi-VM deployments (prod at 25-120 VMs) so a single zone outage doesn't take down the fleet. ANY lets GCP place the VM in whichever zone has capacity — right for target_size=1 (dev) because EVEN anchors a single VM to one zone and can't fall back on capacity exhaustion. BALANCED is a middle option. See https://cloud.google.com/compute/docs/instance-groups/distributing-instances-with-regional-instance-groups."
  type        = string
  default     = "EVEN"

  validation {
    condition     = contains(["EVEN", "BALANCED", "ANY", "ANY_SINGLE_ZONE"], var.distribution_policy_target_shape)
    error_message = "Must be one of: EVEN, BALANCED, ANY, ANY_SINGLE_ZONE."
  }
}

variable "enable_autohealing" {
  description = "If true, attach a google_compute_health_check that probes /healthz on port 8080, start a same-image VM Health agent on host port 8080, and wire the check into the MIG's auto_healing_policies. The agent probes all configured worker /healthz endpoints and applies 600s continuous all-workers-unhealthy hysteresis before returning VM-unhealthy. VMs that fail 3 consecutive GCP probes after the initial_delay_sec window are replaced. The consumer is responsible for adding a firewall rule that allows GCP probe sources (130.211.0.0/22, 35.191.0.0/16) to reach port 8080 on the instances. Without the firewall, every VM is flagged unhealthy and the autohealer recreates instances in a loop."
  type        = bool
  default     = false
}

variable "tmpfs_mounts" {
  description = "Optional list of tmpfs mounts to pass to the docker container as --tmpfs flags."
  type        = list(string)
  default     = []
}

variable "cos_image_self_link" {
  description = "The self_link of the COS image to use. If not specified, the module will query the latest stable COS image."
  type        = string
  default     = null
}

variable "available_zones" {
  description = "The list of zones in the region. If not specified, the module will query the available zones in the region."
  type        = list(string)
  default     = null
}

