resource "google_compute_resource_policy" "daily_stop" {
  name    = "${var.name}-daily-stop"
  project = var.project_id
  region  = join("-", slice(split("-", var.zone), 0, 2))

  instance_schedule_policy {
    vm_stop_schedule {
      schedule = "0 ${var.stop_hour} * * *"
    }
    vm_start_schedule {
      schedule = "0 ${var.start_hour} * * *"
    }
    time_zone = "America/Los_Angeles"
  }
}

resource "google_compute_instance" "eval_instance" {
  name         = var.name
  project      = var.project_id
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "projects/deeplearning-platform-release/global/images/family/common-cu121-debian-11"
      size  = 100 # Deep Learning images are large, 100GB recommended
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {
      # Ephemeral public IP for SSH access
    }
  }

  guest_accelerator {
    type  = var.gpu_type
    count = var.gpu_count
  }

  scheduling {
    on_host_maintenance = "TERMINATE" # Required for GPU instances
    automatic_restart   = true
  }

  resource_policies = [google_compute_resource_policy.daily_stop.id]

  service_account {
    # Best practice is to use a dedicated service account, but defaulting to compute default for simplicity if not specified.
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  tags = ["asr-eval", "jupyter"]

  # Ensure the resource policy is created before attaching it
  depends_on = [google_compute_resource_policy.daily_stop]
}
