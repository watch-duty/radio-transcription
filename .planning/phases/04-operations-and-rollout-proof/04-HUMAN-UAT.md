---
status: blocked
phase: 04-operations-and-rollout-proof
source:
  - 04-VERIFICATION.md
started: 2026-06-27T05:53:24Z
updated: 2026-06-27T19:20:00Z
---

# Human UAT: Phase 04 Operations and Rollout Proof

## Current Test

Live dev/staging verification attempted from local credentials. The proof is
blocked before the feed mutation because the Feed Audit Notification route is
not deployed in the dev project yet.

## Tests

### 1. Dev/staging Feed Audit Notification proof

expected: Follow `docs/feed-audit-notification-rollout.md` against live dev/staging. A disposable echo feed mutation produces one producer log, a routed Pub/Sub LogEntry with the same `event_id`, relay delivery logs, and a Watch Duty 2xx response. The debug subscription is cleaned up on success and failure.
result: blocked

evidence:
- Dev project resolved from deployment Terraform: `probable-symbol-492218-i7`.
- Dev API gateway exists: `radio-transcription-api-gateway-dev` is `ACTIVE`.
- Dev Cloud Run API/feed services exist, but `feed-audit-webhook-dev` is not deployed.
- `gcloud logging sinks describe feed-audit-notification-route-dev --project=probable-symbol-492218-i7` returned `NOT_FOUND`.
- `gcloud pubsub topics list --project=probable-symbol-492218-i7 | rg 'feed-audit-notification'` returned no route topics.
- Local `terraform plan -input=false` cannot run because required variables are only supplied by GitHub environment variables/secrets, including `WD_BACKEND_ENDPOINT`, `WD_BACKEND_ENDPOINT_API_KEY`, bucket names, Broadcastify credentials, Google OAuth values, and notification credentials.

next:
- Push the deployment branch and run the deployment repo's `Deploy Infrastructure` workflow for `environment=dev`, `deploy_release=true`, with the desired public repo SHA.
- Then rerun the dev/staging proof.

### 2. Controlled staging DLQ and restore proof

expected: Follow the runbook's controlled staging DLQ proof using an approved staging-only WD failure endpoint or bad credential reference. The failing event reaches `feed-audit-notification-dlq-subscription-dev`; then rollback/restore is proven by a second disposable feed mutation with WD 2xx delivery.
result: blocked

evidence:
- Depends on the Feed Audit Notification route, push subscription, DLQ, and relay being deployed first.
- Also requires an explicit approved staging-only WD failure endpoint or bad credential reference and a restore/rollback action against live staging configuration.

next:
- Run only after the dev route deployment succeeds.
- Require an explicit operator-approved failure mode before mutating staging relay configuration.

## Summary

total: 2
passed: 0
issues: 0
pending: 0
skipped: 0
blocked: 2

## Gaps

- Dev/staging route infrastructure is not yet deployed from the deployment repo branch.
- Local Terraform apply is intentionally not possible without GitHub environment variables/secrets.
