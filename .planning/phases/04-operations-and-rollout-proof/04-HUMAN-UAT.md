---
status: blocked
phase: 04-operations-and-rollout-proof
source:
  - 04-VERIFICATION.md
started: 2026-06-27T05:53:24Z
updated: 2026-06-27T14:18:00Z
---

# Human UAT: Phase 04 Operations and Rollout Proof

## Current Test

Live dev deployment was completed and route resources were verified. The
end-to-end proof is now blocked at the disposable feed mutation because local
operator credentials cannot mint the user-email BFF token or impersonated
service-account audience token needed for an authenticated audited mutation.

## Tests

### 1. Dev/staging Feed Audit Notification proof

expected: Follow `docs/feed-audit-notification-rollout.md` against live dev/staging. A disposable echo feed mutation produces one producer log, a routed Pub/Sub LogEntry with the same `event_id`, relay delivery logs, and a Watch Duty 2xx response. The debug subscription is cleaned up on success and failure.
result: blocked

evidence:
- Dev project resolved from deployment Terraform: `probable-symbol-492218-i7`.
- Dev API gateway exists: `radio-transcription-api-gateway-dev` is `ACTIVE`.
- Deployment branch `gsd/phase-02-cloud-logging-and-pub-sub-routing` was pushed to `watch-duty/radio-transcription-deployment`.
- Public source SHA `a6c663e815f146c1d480c772d509814e3d328c67` was pushed under `gsd/feed-audit-notification-public-20260627`.
- First dev infra workflow run `28290929662` failed because the Terraform deployer lacked `logging.sinks.create`.
- Deployment fix `1ee4264` grants the deployer `roles/logging.configWriter` and makes the route wait for that IAM binding.
- Dev infra workflow run `28291121846` passed plan, apply, and triggered app deployment.
- Full dev app workflow run `28291244373` succeeded, including Firebase hosting and existing service deploys.
- Branch app workflow run `28291535856` deployed `feed_audit_webhook` successfully.
- Live sink `feed-audit-notification-route-dev` exists and routes `radio_transcription.feed_audit_notification` schema version `1` to `feed-audit-notification-dev`.
- Live Pub/Sub topics/subscriptions exist: `feed-audit-notification-dev`, `feed-audit-notification-dlq-dev`, `feed-audit-notification-subscription-dev`, and `feed-audit-notification-dlq-subscription-dev`.
- Live Cloud Run service `feed-audit-webhook-dev` is Ready at revision `feed-audit-webhook-dev-00002-fgm`.
- Temporary proof subscriptions were cleaned up after failed mutation attempts.
- BFF-path proof using `gcloud auth print-identity-token` returned HTTP 403 because the local token lacks the user email claim needed by `feedMutationActorHeaders`.
- Direct feeds-service proof with local user token returned HTTP 401 because local user credentials cannot mint an audience-bound Cloud Run ID token.
- Attempted impersonation of `radio-transcription-api-dev@probable-symbol-492218-i7.iam.gserviceaccount.com` failed with `iam.serviceAccounts.getAccessToken` denied.

next:
- Rerun the disposable feed mutation proof from a browser-authenticated admin BFF session, or grant a controlled temporary impersonation path that can mint an audience-bound token for the dev feeds service.
- Then verify producer log, routed Pub/Sub LogEntry, relay delivery log, and WD 2xx for the captured `event_id`.

### 2. Controlled staging DLQ and restore proof

expected: Follow the runbook's controlled staging DLQ proof using an approved staging-only WD failure endpoint or bad credential reference. The failing event reaches `feed-audit-notification-dlq-subscription-dev`; then rollback/restore is proven by a second disposable feed mutation with WD 2xx delivery.
result: blocked

evidence:
- Route infrastructure and relay are now deployed in dev.
- Also requires an explicit approved staging-only WD failure endpoint or bad credential reference and a restore/rollback action against live staging configuration.

next:
- Run only after the successful event delivery proof completes.
- Require an explicit operator-approved failure mode before mutating staging relay configuration.

## Summary

total: 2
passed: 0
issues: 0
pending: 0
skipped: 0
blocked: 2

## Gaps

- End-to-end feed mutation proof needs a credential path that can create an audited feed through the BFF or feeds-service without bypassing the intended security boundary.
- Controlled DLQ/restore proof still needs an explicit approved staging failure mode.
