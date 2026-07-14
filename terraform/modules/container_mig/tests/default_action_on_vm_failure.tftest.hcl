mock_provider "google" {}

variables {
  project_id            = "test-project"
  region                = "us-central1"
  name_prefix           = "test-mig"
  subnetwork_id         = "projects/test-project/regions/us-central1/subnetworks/test"
  service_account_email = "test@test-project.iam.gserviceaccount.com"
  container_image       = "us-central1-docker.pkg.dev/test-project/repo/image:tag"
}

run "normal_repair_is_the_default" {
  command = plan

  assert {
    condition = (
      google_compute_region_instance_group_manager.this.instance_lifecycle_policy[0].default_action_on_failure
      == "REPAIR"
    )
    error_message = "The default MIG VM-failure action must remain REPAIR."
  }
}

run "maintenance_can_disable_vm_failure_repair" {
  command = plan

  variables {
    default_action_on_vm_failure = "DO_NOTHING"
  }

  assert {
    condition = (
      google_compute_region_instance_group_manager.this.instance_lifecycle_policy[0].default_action_on_failure
      == "DO_NOTHING"
    )
    error_message = "Maintenance must render DO_NOTHING for VM failures."
  }
}

run "invalid_failure_action_is_rejected" {
  command = plan

  variables {
    default_action_on_vm_failure = "INVALID"
  }

  expect_failures = [var.default_action_on_vm_failure]
}
