resource "google_storage_bucket" "this" {
  name                        = var.name
  project                     = var.project_id
  location                    = var.location
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy
  labels                      = var.labels

  soft_delete_policy {
    retention_duration_seconds = var.enable_soft_delete ? 7 * 24 * 60 * 60 : 0
  }

  dynamic "cors" {
    for_each = var.cors

    content {
      origin          = cors.value.origin
      method          = cors.value.method
      response_header = cors.value.response_header
      max_age_seconds = cors.value.max_age_seconds
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      action {
        type          = lifecycle_rule.value.action.type
        storage_class = lifecycle_rule.value.action.storage_class
      }
      condition {
        age                   = lifecycle_rule.value.condition.age
        created_before        = lifecycle_rule.value.condition.created_before
        with_state            = lifecycle_rule.value.condition.with_state
        num_newer_versions    = lifecycle_rule.value.condition.num_newer_versions
        matches_storage_class = lifecycle_rule.value.condition.matches_storage_class
        matches_prefix        = lifecycle_rule.value.condition.matches_prefix
      }
    }
  }
}
