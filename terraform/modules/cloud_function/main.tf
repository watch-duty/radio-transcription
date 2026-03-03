# 1. Zip the Python code directory
data "archive_file" "source" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "/tmp/function-${var.function_name}.zip"
}

# 2. Upload the zip to a Google Cloud Storage bucket
resource "google_storage_bucket_object" "zip" {
  name   = "${var.function_name}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket_name
  source = data.archive_file.source.output_path
}

# 3. Create the Gen 2 Cloud Function
resource "google_cloudfunctions2_function" "default" {
  name        = var.function_name
  location    = var.region
  description = var.description

  build_config {
    runtime     = var.runtime
    entry_point = var.entry_point
    source {
      storage_source {
        bucket = var.source_bucket_name
        object = google_storage_bucket_object.zip.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "256M"
    timeout_seconds       = 60
    environment_variables = var.environment_variables
  }

  # If you pass a trigger_topic_id, it creates the Pub/Sub trigger. 
  # If you don't, it defaults to a standard HTTP function.
  dynamic "event_trigger" {
    for_each = var.trigger_topic_id != null ? [var.trigger_topic_id] : []
    content {
      trigger_region = var.region
      event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
      pubsub_topic   = event_trigger.value
      retry_policy   = "RETRY_POLICY_DO_NOT_RETRY"
    }
  }
}