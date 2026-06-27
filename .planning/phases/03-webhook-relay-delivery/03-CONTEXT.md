# Phase 3: Webhook Relay Delivery - Context

**Gathered:** 2026-06-27
**Status:** Ready for planning

## Phase Boundary

Phase 3 implements the stateless Feed Audit Webhook relay. The relay receives
authenticated Pub/Sub push requests from the Phase 2 Cloud Logging route,
decodes the Cloud Logging `LogEntry`, extracts the Feed Audit Notification
`jsonPayload`, shallow-validates the v1 event contract, and forwards that flat
payload to the Watch Duty backend endpoint:

`/api/v1/echo/radio_transcription/internal/audit/webhook/`

Phase 3 owns the public relay application code, package, Dockerfile, tests, WD
client behavior, Pub/Sub envelope parsing, and relay configuration contract.
It does not change producer audit SQL, add database reads/writes, add delivery
tables, poll AlloyDB, implement replay tooling, build dashboards/runbooks, or
add UI behavior. Deployment-specific Cloud Run resources, secrets, and concrete
environment values belong in the deployment repo unless a Terraform component
is genuinely reusable.

## Implementation Decisions

### Relay Package Shape
- **D-01:** Create a dedicated public Python service package at
  `backend/pipeline/feed_audit_webhook`, rather than extending the existing
  `backend/pipeline/notification` package.
- **D-02:** Use `feed_audit_webhook` for Python package/directory naming and
  `feed-audit-webhook` for service-style resource naming. Avoid the longer
  `feed_audit_webhook_relay` name unless an existing deployment convention
  requires it.
- **D-03:** Implement the relay as a FastAPI + Uvicorn HTTP service, matching
  the existing Pub/Sub push Cloud Run service pattern in transcription.
- **D-04:** Use `urllib3` directly for outbound WD calls, matching the existing
  notification sender dependency style and avoiding a new HTTP dependency.

### Pub/Sub Envelope Validation
- **D-05:** The production endpoint must be strict: accept the standard Pub/Sub
  push envelope, decode `message.data` as base64 JSON, parse it as a Cloud
  Logging `LogEntry`, and require an object `jsonPayload`.
- **D-06:** Shallow-validate only the v1 Feed Audit Notification contract:
  `event_type`, `schema_version`, `event_id`, `action`, `occurred_at`,
  `actor_id`, `feed_id`, `feed_revision`, `before_values`, and `after_values`.
  Verify `event_type="radio_transcription.feed_audit_notification"` and
  `schema_version=1`.
- **D-07:** Do not deeply validate audit snapshot fields or action-specific
  before/after details. That would duplicate storage-layer schema knowledge and
  increase long-term maintenance cost.
- **D-08:** Return non-2xx for malformed Pub/Sub envelopes, malformed LogEntry
  data, missing `jsonPayload`, unsupported event type, unsupported schema
  version, or missing required v1 fields so Pub/Sub retries and eventually DLQs.
- **D-09:** Forward the extracted flat `jsonPayload` unchanged to WD. Do not
  normalize UUIDs/timestamps, wrap in another envelope, or add fields in the
  relay.

### WD Forwarding And Retry Behavior
- **D-10:** Locally retry only transient send failures: timeout, connection
  failure, HTTP `408`, HTTP `429`, and HTTP `5xx`.
- **D-11:** Perform two total WD POST attempts. Do not locally retry `400`,
  `422`, `401`, `403`, or other non-transient `4xx` responses.
- **D-12:** Use an outbound request timeout of about 15 seconds per WD POST
  attempt.
- **D-13:** Use a tiny jittered local delay, about 250-500 ms, between the two
  attempts. Do not add seconds-level local backoff because Pub/Sub already owns
  the outer retry budget.
- **D-14:** Only WD `2xx` responses should return HTTP `204` to Pub/Sub.
  Every relay validation failure, unsupported message, WD auth/config failure,
  non-transient WD `4xx`, and exhausted transient WD failure should return
  non-2xx so Pub/Sub retries and eventually DLQs.
- **D-15:** Log the full WD response body on failures for contract debugging.
  Do not log the API key, and avoid adding separate duplicate request-payload
  logs beyond event identifiers and the normal structured context.
- **D-16:** Planning must resolve the Phase 2 ack-deadline mismatch. Phase 2
  currently configured a 10 second Pub/Sub push ack deadline, but the chosen
  local send budget can take roughly 30 seconds plus jitter. Either Phase 3 must
  update the route/deployment ack deadline or planning must explicitly revise
  the timeout choice with user approval.

### Configuration And Secret Contract
- **D-17:** Configure the relay with `WD_BACKEND_BASE_URL`; keep the webhook
  path fixed in code as
  `/api/v1/echo/radio_transcription/internal/audit/webhook/`.
- **D-18:** Configure the outbound credential with `WD_BACKEND_API_KEY`, sent
  as the `X-Api-Key` header.
- **D-19:** Validate required configuration at startup. Missing or malformed
  `WD_BACKEND_BASE_URL` or missing `WD_BACKEND_API_KEY` should prevent a healthy
  service startup rather than creating per-message retry storms.
- **D-20:** Hardcode the relay's accepted event type and schema version as code
  constants. Do not make `event_type` or `schema_version` environment
  configurable because the producer, sink, and relay must stay aligned.
- **D-21:** Reusable relay application code, tests, constants, payload parsing,
  WD client behavior, Dockerfile/package, and env-var contract documentation
  belong in the public repo. Environment-specific Cloud Run service resources,
  Secret Manager bindings/values, route enablement, and concrete base URL/API
  key wiring belong in the deployment repo.
- **D-22:** If a Cloud Run service Terraform module or contract can be made
  genuinely reusable without private deployment assumptions, put the reusable
  module/contract in the public repo. Otherwise, document the public contract
  and keep concrete Terraform wiring in the deployment repo.

### The Agent's Discretion

The planner may choose exact class/function names, exception names, test file
layout, and whether the endpoint returns a specific `4xx`/`5xx` status for
different failure classes. Preserve the locked ACK/NACK semantics above.

## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project And Requirements
- `.planning/PROJECT.md` - Defines the milestone, critical-path constraint,
  best-effort notification framing, WD endpoint, and public/deployment split.
- `.planning/REQUIREMENTS.md` - Lists Phase 3 requirements `RELAY-01..06` and
  adjacent Phase 4 operations requirements.
- `.planning/ROADMAP.md` - Defines Phase 3 success criteria and boundaries.
- `.planning/STATE.md` - Captures accumulated decisions from Phases 1 and 2.
- `.planning/phases/01-audit-contract-and-emission/01-CONTEXT.md` - Locks the
  producer payload contract, event type, schema version, and non-critical path
  constraints.
- `.planning/phases/02-cloud-logging-and-pub-sub-routing/02-CONTEXT.md` -
  Locks the Cloud Logging to Pub/Sub route shape, IAM, retry, DLQ, and relay
  input contract.

### Producer And Payload Contract
- `backend/pipeline/storage/feed_audit_notifications.py` - Shared producer
  helper and accepted Feed Audit Notification required fields.
- `backend/pipeline/storage/feed_audit_sql.py` - SQL payload construction for
  `event_type`, `schema_version`, and flat audit event fields.
- `backend/pipeline/storage/tests/test_feed_audit_notifications.py` - Producer
  payload/logging contract tests.
- `backend/pipeline/storage/tests/test_feed_query_contracts.py` - SQL contract
  tests proving audited mutations return `feed_audit_event`.

### Existing HTTP And Pub/Sub Push Patterns
- `backend/pipeline/transcription/main.py` - FastAPI Pub/Sub push HTTP service
  pattern returning `204` on successful processing.
- `backend/pipeline/notification/request_handler.py` - Existing `urllib3`
  outbound `X-Api-Key` webhook sender pattern.
- `backend/pipeline/notification/send_notification.py` - Existing platform
  retry behavior for transient versus non-retryable notification failures.
- `backend/pipeline/common/clients/session_helper.py` - Existing retry status
  vocabulary including `429` and `5xx`.
- `backend/pipeline/ingestion/failure_classifiers/http_status.py` - Ingestion
  transient HTTP status classification for `408`, `429`, and `5xx`.
- `backend/pipeline/ingestion/collector_runtime.py` - VM ingestion failure
  isolation pattern; feed failures record state and do not crash the worker.
- `backend/pipeline/ingestion/collectors/echo/main.py` - Echo behavior where
  return versus raise controls platform retry.

### Deployment Route Outputs From Phase 2
- `../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf` - Dedicated notification topic and DLQ topic.
- `../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf` - Log sink, push subscription, OIDC push identity, retry, DLQ, and current ack deadline.
- `../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/variables.tf` - Relay URL/name contract consumed by the route module.
- `../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf` - App-level route module wiring.
- `../../feed-audit-notification-routing-deployment/terraform/modules/app/variables.tf` - App-level relay route inputs.
- `../../feed-audit-notification-routing-deployment/terraform/modules/app/outputs.tf` - Exposed route outputs for later deployment/verification.

## Existing Code Insights

### Reusable Assets
- `backend.pipeline.common.log_helper.setup_logging()` should be used by the
  new service so logs land in the existing structured Cloud Logging setup.
- `backend/pipeline/transcription/main.py` provides the closest FastAPI Pub/Sub
  push shape.
- `backend/pipeline/notification/request_handler.py` provides the closest
  outbound `urllib3` + `X-Api-Key` pattern, but Phase 3 should not copy its
  exact retry/status behavior because relay requirements are stricter.
- `backend/pipeline/storage/feed_audit_notifications.py` already centralizes
  the v1 required-field vocabulary that the relay can mirror in tests or shared
  constants if planning finds a clean import boundary.

### Established Patterns
- Backend pipeline services use small package-scoped `pyproject.toml` files and
  Dockerfiles that export locked dependencies with `uv`.
- Cloud Run HTTP services use FastAPI/Uvicorn where request/response semantics
  matter.
- Existing pipeline code treats `429` and `5xx` as transient. Ingestion also
  treats `408` as retryable.
- Platform retry is normally triggered by raising or returning non-success from
  the entry point; ACK/drop behavior is explicit and should be tested.

### Integration Points
- Add `backend/pipeline/feed_audit_webhook/pyproject.toml`,
  `backend/pipeline/feed_audit_webhook/Dockerfile`, package modules, and tests
  in the public repo.
- Add the new package to the root workspace/package dependencies as needed so
  `uv lock`, lint, type checking, and tests include it.
- Deployment repo Phase 3 work should instantiate the Cloud Run service and
  supply `relay_service_url` and `relay_service_name` to the Phase 2 route
  module.
- Deployment repo work must supply `WD_BACKEND_BASE_URL` and
  `WD_BACKEND_API_KEY` from environment-specific config/Secret Manager.

## Specific Ideas

Prefer an internal structure similar to:

```text
backend/pipeline/feed_audit_webhook/
  Dockerfile
  pyproject.toml
  main.py
  settings.py
  pubsub.py
  wd_client.py
  tests/
```

The exact module split is discretionary. Keep the production endpoint strict:

`POST /pubsub/feed-audit-notifications`

The relay should POST the extracted payload unchanged to:

`{WD_BACKEND_BASE_URL}/api/v1/echo/radio_transcription/internal/audit/webhook/`

## Deferred Ideas

- Replay selected `feed_audit_events` rows by event ID or time range remains
  v2 scope.
- Delivery attempt history outside Cloud Logging/Pub/Sub DLQ remains v2 scope.
- Stronger HMAC-style outbound webhook authentication remains v2 scope unless
  WD endpoint exposure requirements change.
- Multi-destination fanout remains v2 scope.
- Operational dashboards, staging proof, production rollout runbook, and DLQ
  inspection guidance belong to Phase 4.

---

*Phase: 3-Webhook Relay Delivery*
*Context gathered: 2026-06-27*
