data "google_project" "project" {}

locals {
  project_id = data.google_project.project.project_id
}

# =============================================================================
# FEED CHANGE NOTIFICATIONS WEBHOOK RELAY
# =============================================================================

module "webhook_relay" {
  source = "./webhook_relay"

  region                         = var.region
  environment                    = var.environment
  webhook_url                    = var.webhook_url
  webhook_api_key                = var.webhook_api_key
  deployer_service_account_email = var.deployer_service_account_email
}

module "notification" {
  source = "./notification"

  region                         = var.region
  environment                    = var.environment
  relay_service_name             = module.webhook_relay.feed_change_webhook_service_name
  relay_service_url              = module.webhook_relay.feed_change_webhook_service_url
  deployer_service_account_email = var.deployer_service_account_email
}

module "monitoring" {
  source = "./monitoring"

  relay_service_name                     = module.webhook_relay.feed_change_webhook_service_name
  environment                            = var.environment
  push_subscription_name                 = module.notification.push_subscription_name
  dlq_subscription_name                  = module.notification.dlq_subscription_name
  slack_critical_notification_channel_id = var.slack_critical_notification_channel_id
}