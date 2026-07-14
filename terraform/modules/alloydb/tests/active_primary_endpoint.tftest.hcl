mock_provider "google" {}
mock_provider "null" {}

variables {
  project_id              = "test-project"
  region                  = "us-central1"
  cluster_id              = "test-alloydb"
  network_id              = "projects/test-project/global/networks/test"
  initial_user_password   = "not-a-real-password"
  worker_user_password    = "not-a-real-worker-password"
  apply_schema            = true
  password_secret_id      = "test-postgres-password"
  password_secret_version = "7"
  sql_job_image           = "mirror.gcr.io/library/postgres@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  subnetwork_id           = "projects/test-project/regions/us-central1/subnetworks/test"
}

override_resource {
  target          = google_alloydb_instance.primary
  override_during = plan
  values = {
    ip_address = "10.20.0.10"
  }
}

run "default_keeps_source_primary_for_both_operation_jobs" {
  command = plan

  assert {
    condition     = output.primary_instance_ip == "10.20.0.10"
    error_message = "The managed source output must remain the source primary."
  }

  assert {
    condition     = output.active_primary_instance_ip == "10.20.0.10"
    error_message = "A null override must select the managed source primary."
  }

  assert {
    condition = one([
      for env in google_cloud_run_v2_job.schema_migration[0].template[0].template[0].containers[0].env : env.value
      if env.name == "DB_HOST"
    ]) == "10.20.0.10"
    error_message = "The schema migration job must use the selected active primary."
  }

  assert {
    condition = one([
      for env in google_cloud_run_v2_job.bcfy_calls_sid_operation[0].template[0].template[0].containers[0].env : env.value
      if env.name == "DB_HOST"
    ]) == "10.20.0.10"
    error_message = "The controlled SID operation job must use the selected active primary."
  }

  assert {
    condition = alltrue([
      google_cloud_run_v2_job.schema_migration[0].template[0].template[0].containers[0].image == var.sql_job_image,
      google_cloud_run_v2_job.bcfy_calls_sid_operation[0].template[0].template[0].containers[0].image == var.sql_job_image,
    ])
    error_message = "Both SQL jobs must use the same immutable image input."
  }

  assert {
    condition = alltrue([
      one([
        for env in google_cloud_run_v2_job.schema_migration[0].template[0].template[0].containers[0].env : env.value_source[0].secret_key_ref[0].version
        if env.name == "PGPASSWORD"
      ]) == var.password_secret_version,
      one([
        for env in google_cloud_run_v2_job.bcfy_calls_sid_operation[0].template[0].template[0].containers[0].env : env.value_source[0].secret_key_ref[0].version
        if env.name == "PGPASSWORD"
      ]) == var.password_secret_version,
    ])
    error_message = "Both SQL jobs must use the same numeric password version input."
  }
}

run "restored_override_moves_both_jobs_but_preserves_source" {
  command = plan

  variables {
    active_primary_instance_ip = "10.42.0.17"
  }

  assert {
    condition     = output.primary_instance_ip == "10.20.0.10"
    error_message = "Selecting a restored primary must not redefine the managed source."
  }

  assert {
    condition     = output.active_primary_instance_ip == "10.42.0.17"
    error_message = "The active output must expose the selected restored primary."
  }

  assert {
    condition = alltrue([
      one([
        for env in google_cloud_run_v2_job.schema_migration[0].template[0].template[0].containers[0].env : env.value
        if env.name == "DB_HOST"
      ]) == "10.42.0.17",
      one([
        for env in google_cloud_run_v2_job.bcfy_calls_sid_operation[0].template[0].template[0].containers[0].env : env.value
        if env.name == "DB_HOST"
      ]) == "10.42.0.17",
    ])
    error_message = "Every database operation job must move to the same restored primary."
  }
}

run "empty_override_is_rejected" {
  command = plan

  variables {
    active_primary_instance_ip = ""
  }

  expect_failures = [var.active_primary_instance_ip]
}

run "public_ipv4_override_is_rejected" {
  command = plan

  variables {
    active_primary_instance_ip = "8.8.8.8"
  }

  expect_failures = [var.active_primary_instance_ip]
}

run "host_port_override_is_rejected" {
  command = plan

  variables {
    active_primary_instance_ip = "10.42.0.17:5432"
  }

  expect_failures = [var.active_primary_instance_ip]
}

run "whitespace_wrapped_override_is_rejected" {
  command = plan

  variables {
    active_primary_instance_ip = " 10.42.0.17 "
  }

  expect_failures = [var.active_primary_instance_ip]
}

run "zero_padded_override_is_rejected" {
  command = plan

  variables {
    active_primary_instance_ip = "192.168.001.001"
  }

  expect_failures = [var.active_primary_instance_ip]
}

run "mutable_sql_job_image_is_rejected" {
  command = plan

  variables {
    sql_job_image = "postgres:16-alpine"
  }

  expect_failures = [var.sql_job_image]
}

run "bare_digest_sql_job_image_is_rejected" {
  command = plan

  variables {
    sql_job_image = "@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }

  expect_failures = [var.sql_job_image]
}

run "whitespace_sql_job_image_is_rejected" {
  command = plan

  variables {
    sql_job_image = "mirror.gcr.io/library/postgres @sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }

  expect_failures = [var.sql_job_image]
}

run "nonnumeric_password_secret_version_is_rejected" {
  command = plan

  variables {
    password_secret_version = "latest"
  }

  expect_failures = [var.password_secret_version]
}
