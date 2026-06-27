---
phase: 03-webhook-relay-delivery
plan: 04
subsystem: deployment
tags: [terraform, cloud-run, secret-manager, pubsub, github-actions]
requires:
  - phase: 03-webhook-relay-delivery
    provides: Plans 03-01 through 03-03 public relay service
provides:
  - Deployment repo Cloud Run service module for `feed-audit-webhook`
  - Secret Manager-backed WD API key wiring and derived WD backend base URL
  - App module route wiring from relay service outputs
  - Pub/Sub push ack deadline raised to 60 seconds
  - App deployment workflow support for `feed_audit_webhook`
affects: [phase-03, deployment-repo]
tech-stack:
  added: [Cloud Run, Secret Manager]
  patterns: [minimal runtime IAM, app-module service composition, public package deploy workflow]
key-files:
  created:
    - radio-transcription-deployment/terraform/modules/services/feed_audit_webhook/main.tf
    - radio-transcription-deployment/terraform/modules/services/feed_audit_webhook/variables.tf
    - radio-transcription-deployment/terraform/modules/services/feed_audit_webhook/outputs.tf
    - radio-transcription-deployment/terraform/modules/services/feed_audit_webhook/versions.tf
  modified:
    - radio-transcription-deployment/terraform/modules/app/main.tf
    - radio-transcription-deployment/terraform/modules/app/variables.tf
    - radio-transcription-deployment/terraform/modules/app/outputs.tf
    - radio-transcription-deployment/terraform/modules/feed_audit_notification_route/main.tf
    - radio-transcription-deployment/.github/workflows/app_deploy.yml
    - radio-transcription-deployment/.github/workflows/terraform_deploy.yml
key-decisions:
  - "Keep Cloud Run service, Secret Manager, IAM, and workflow wiring in the deployment repo."
  - "Reuse `WD_BACKEND_ENDPOINT_API_KEY` for the relay API key and derive the default base URL from the existing `WD_BACKEND_ENDPOINT` origin."
  - "Do not grant the relay runtime service account AlloyDB, Redis, storage, Pub/Sub data-plane, or VPC access."
patterns-established:
  - "Route module consumes relay service URI/name from the service module rather than manual placeholder variables."
  - "Terraform route ack deadline is 60 seconds to cover the relay's two local WD attempts plus jitter/overhead."
requirements-completed:
  - RELAY-01
  - RELAY-03
  - RELAY-04
  - RELAY-05
  - RELAY-06
duration: 25min
completed: 2026-06-27
---

# Phase 03 Plan 04: Deployment Wiring Summary

**Cloud Run relay service and Pub/Sub route deployment wiring**

## Accomplishments

- Added a deployment repo service module for `feed-audit-webhook-${environment}` with a dedicated runtime service account.
- Injected `WD_BACKEND_BASE_URL`, `WD_BACKEND_API_KEY`, `IS_GCP`, and `GOOGLE_CLOUD_PROJECT` into the Cloud Run service. The API key is stored in Secret Manager and exposed through `value_source.secret_key_ref`.
- Granted the relay runtime service account only Secret Manager access for its own WD key plus logging, monitoring, and trace writer roles.
- Wired the app module route to the relay service module outputs, replacing the previous manual relay URL/name inputs.
- Raised the Feed Audit Notification Pub/Sub push ack deadline from 10 seconds to 60 seconds.
- Updated app deploy workflow service choices, path filters, all-service matrix, and Cloud Run target mapping for `feed_audit_webhook`.

## Task Commits

1. `e25449b` in `radio-transcription-deployment` - `feat: deploy feed audit webhook relay`

## Verification

From `/home/shuojing/watch-duty-repo/.worktrees/feed-audit-notification-routing-deployment`:

- `safe-run -- terraform fmt terraform/modules/services/feed_audit_webhook terraform/modules/app terraform/modules/feed_audit_notification_route`
- `safe-run -- terraform fmt -check terraform/modules/services/feed_audit_webhook terraform/modules/app terraform/modules/feed_audit_notification_route`
- `safe-run -- terraform -chdir=terraform/environments/dev init -backend=false`
- `safe-run -- terraform -chdir=terraform/environments/prod init -backend=false`
- `safe-run -- terraform -chdir=terraform/environments/dev validate`
- `safe-run -- terraform -chdir=terraform/environments/prod validate`
- `rg -n 'feed-audit-webhook-\$\{var\.environment\}|WD_BACKEND_BASE_URL|WD_BACKEND_API_KEY|roles/logging\.logWriter|roles/monitoring\.metricWriter|roles/cloudtrace\.agent|secretAccessor' terraform/modules/services/feed_audit_webhook`
- `rg -n 'module "feed_audit_webhook"|feed_audit_webhook_service_url|feed_audit_webhook_service_name|ack_deadline_seconds = 60|relay_service_url\s+= module\.feed_audit_webhook|relay_service_name\s+= module\.feed_audit_webhook' terraform/modules/app terraform/modules/feed_audit_notification_route`
- `rg -n 'feed_audit_webhook|feed-audit-webhook' .github/workflows/app_deploy.yml .github/workflows/terraform_deploy.yml`
- `rg -n 'alloydb|redis|roles/pubsub|roles/storage|vpc_access' terraform/modules/services/feed_audit_webhook` returned no matches.

## Deviations From Plan

- Instead of adding new required GitHub secrets/variables, the module reuses the existing `WD_BACKEND_ENDPOINT_API_KEY` secret and derives the default relay base URL from the existing `WD_BACKEND_ENDPOINT` origin. An optional `feed_audit_webhook_wd_backend_base_url` override remains available.

## User Setup Required

None beyond the existing WD backend endpoint and API key environment configuration. To use a different base URL for this relay, set `feed_audit_webhook_wd_backend_base_url`.
