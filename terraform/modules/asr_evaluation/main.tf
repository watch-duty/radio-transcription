
resource "google_compute_instance" "eval_instance" {
  name         = var.name
  project      = var.project_id
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "projects/deeplearning-platform-release/global/images/pytorch-2-7-cu128-ubuntu-2204-nvidia-570-v20260320"
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
    on_host_maintenance         = "TERMINATE" # Required for GPU instances
    automatic_restart           = false
    provisioning_model          = var.provisioning_model
    preemptible                 = var.provisioning_model == "SPOT" ? true : false
    instance_termination_action = var.provisioning_model == "SPOT" ? "STOP" : ""
  }

  resource_policies = []

  metadata = {
    startup_script = <<-EOT
    #!/bin/bash
    sleep $(( ${var.auto_shutdown_hours} * 3600 ))
    while true; do
      if nvidia-smi --query-compute-apps=pid --format=csv,noheader | grep -q . ; then
        sleep 1800
      else
        sudo poweroff
        break
      fi
    done
  EOT
  }

  service_account {
    # Best practice is to use a dedicated service account, but defaulting to compute default for simplicity if not specified.
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  tags = ["asr-eval", "jupyter"]
}
