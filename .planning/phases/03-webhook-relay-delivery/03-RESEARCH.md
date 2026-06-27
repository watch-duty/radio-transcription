# Phase 3: Webhook Relay Delivery - Research

**Researched:** 2026-06-27
**Scope:** Public relay service, Pub/Sub push envelope handling, Watch Duty
webhook forwarding, and deployment coordination.

## Goal-Backward Summary

Phase 3 should add a stateless Cloud Run HTTP service that receives Pub/Sub push
requests from the Phase 2 Cloud Logging route and forwards the `jsonPayload`
from each routed `LogEntry` to the Watch Duty backend webhook. The relay must
not read or write AlloyDB, must not participate in feed writes, and must only
ACK Pub/Sub after Watch Duty returns a `2xx`.

The design is intentionally not a durable audit replication system. Durability
comes from the already-persisted `feed_audit_events` row and the managed
Cloud Logging -> Pub/Sub route. The relay is an adapter from Pub/Sub-delivered
logging events to the WD endpoint.

## Documentation Findings

### Pub/Sub Push

Google Pub/Sub push subscriptions send each message as an HTTPS request to the
configured endpoint. A success HTTP status acknowledges the message; non-success
or request timeout causes Pub/Sub to redeliver according to subscription retry
settings.

The Pub/Sub `ack_deadline_seconds` setting also bounds push request timeout.
Context7 for `googleapis/python-pubsub` confirmed that the value is the
approximate wait time before resending, and that for push delivery it sets the
request timeout. Minimum is 10 seconds and maximum is 600 seconds.

Implication: Phase 2's current `ack_deadline_seconds = 10` is too low for the
locked Phase 3 local budget of two WD POST attempts at about 15 seconds each
plus jitter and processing overhead. Phase 3 must update the deployment route to
use a larger deadline, preferably 60 seconds.

Official references:
- https://docs.cloud.google.com/pubsub/docs/push
- https://docs.cloud.google.com/pubsub/docs/dead-letter-topics

### Cloud Logging Sink To Pub/Sub

Cloud Logging routes matching log entries through Log Router sinks to
destinations such as Pub/Sub. The v1 producer already emits structured logging
fields under `jsonPayload`, and Phase 2 filters on
`jsonPayload.event_type="radio_transcription.feed_audit_notification"` and
`jsonPayload.schema_version=1`.

Implication: the relay should treat the Pub/Sub message body as a Cloud Logging
`LogEntry` and extract only object `jsonPayload`. It should not accept raw audit
payloads on the production endpoint, because that would bypass the actual route
contract.

Official references:
- https://cloud.google.com/logging/docs/routing/overview
- https://cloud.google.com/logging/docs/export/pubsub

## Existing Code Evidence

### HTTP Service Shape

`backend/pipeline/transcription/main.py` is the closest public service analog:
it exposes a FastAPI app, receives Pub/Sub push payloads as JSON, and returns
`204` when processing succeeds. It uses
`backend.pipeline.common.log_helper.setup_logging()` so container logs enter the
existing structured logging path.

Apply to Phase 3:
- create a dedicated FastAPI app in `backend/pipeline/feed_audit_webhook`
- expose `POST /pubsub/feed-audit-notifications`
- return `204` only after successful WD forwarding
- use `setup_logging()` on module import

### Outbound Webhook Shape

`backend/pipeline/notification/request_handler.py` uses `urllib3.PoolManager`
and sends `X-Api-Key` to a Watch Duty endpoint. It has broader retry behavior
than Phase 3 needs, but it is the right dependency and header pattern.

Apply to Phase 3:
- use `urllib3` directly, not a new HTTP client
- configure endpoint with `WD_BACKEND_BASE_URL` and a fixed path
  `/api/v1/echo/radio_transcription/internal/audit/webhook/`
- send the extracted flat audit payload unchanged as JSON
- use `X-Api-Key: $WD_BACKEND_API_KEY`

### Producer Contract

`backend/pipeline/storage/feed_audit_notifications.py` defines the existing
event type, schema version, and required keys:

```text
event_type
schema_version
event_id
action
occurred_at
actor_id
feed_id
feed_revision
before_values
after_values
```

Apply to Phase 3:
- reuse the constants where the import boundary stays clean
- otherwise mirror them in tests with explicit source-inspection coverage
- validate only these shallow fields, not action-specific snapshot internals

### Deployment Route

Phase 2 deployment worktree:
`/home/shuojing/watch-duty-repo/.worktrees/feed-audit-notification-routing-deployment`

Current route module:
`terraform/modules/feed_audit_notification_route/main.tf`

The route already includes:
- `google_logging_project_sink.feed_audit_notification`
- sink writer `roles/pubsub.publisher` on the notification topic
- dedicated Pub/Sub push invoker service account
- Cloud Run `roles/run.invoker` binding for the relay service
- OIDC push config with audience set to relay service base URL
- retry policy `10s` to `60s`
- DLQ policy capped at 10 attempts

Gap for Phase 3:
- instantiate the actual `feed-audit-webhook-${environment}` Cloud Run service
- pass its URI/name into the route module
- increase `ack_deadline_seconds` from 10 to 60
- add app-deploy workflow support for the new public Dockerfile

## Design Constraints

1. **No AlloyDB access in the relay.**
   The relay must not import storage modules, open DB connections, run SQL, or
   create delivery/cursor state.

2. **No direct critical-path delivery.**
   The relay is downstream of Cloud Logging and Pub/Sub. Feed mutation and
   ingestion code must not call the relay or WD directly.

3. **One transformation boundary.**
   Decode Pub/Sub `message.data` once, parse the Cloud Logging `LogEntry` JSON
   once, extract `jsonPayload`, and send that same dict to WD. Do not wrap,
   normalize, or re-encode/decode repeatedly inside producer/relay layers.

4. **Pub/Sub owns outer retry.**
   The relay performs two total local WD attempts only for timeout, connection
   failure, HTTP `408`, `429`, and `5xx`. All other WD failures return
   non-2xx to Pub/Sub without local retry.

5. **Malformed inputs NACK.**
   Malformed Pub/Sub envelopes, malformed base64 data, missing `jsonPayload`,
   wrong event type, wrong schema version, and missing required fields return
   non-2xx so Pub/Sub can retry and eventually DLQ.

## Recommended Implementation Shape

```text
backend/pipeline/feed_audit_webhook/
  Dockerfile
  README.md
  pyproject.toml
  __init__.py
  main.py
  pubsub.py
  settings.py
  wd_client.py
  tests/
```

Public repo responsibilities:
- app package and tests
- fixed WD path and env-var contract docs
- Dockerfile and workspace lock updates
- no Terraform resources unless a component is genuinely reusable

Deployment repo responsibilities:
- Cloud Run service resource
- service account and minimal logging/monitoring/trace IAM
- Secret Manager binding for `WD_BACKEND_API_KEY`
- `WD_BACKEND_BASE_URL` environment value
- route module ack deadline adjustment
- app deploy workflow support for `feed_audit_webhook`

## Verification Strategy

Public repo:
- parser unit tests for valid and invalid Pub/Sub push envelopes
- validation tests for missing required fields, wrong event type, and wrong
  schema version
- WD client tests for URL/path construction, `X-Api-Key`, JSON body, retryable
  status handling, non-retryable status handling, response body logging, and
  no API key logging
- FastAPI endpoint tests using `TestClient` or `httpx` with mocked WD client
- source-inspection test proving the relay package does not import
  `backend.pipeline.storage`, `asyncpg`, or `psycopg`
- Docker build smoke if CI supports it; otherwise py_compile and focused tests

Deployment repo:
- `terraform fmt -check` on changed modules
- `terraform validate` or existing lint workflow if available
- `rg` contract checks for service name, env vars, secret use, route module
  service URL/name wiring, and `ack_deadline_seconds = 60`
- app deploy workflow check that `specific_service` and change detection include
  `feed_audit_webhook`, and service-name mapping resolves to
  `feed-audit-webhook-${environment}`

## Risks And Mitigations

| Risk | Mitigation |
|------|------------|
| Pub/Sub times out before the second WD attempt completes | Raise route `ack_deadline_seconds` to 60 in Phase 3. |
| Relay accidentally accepts raw payloads in prod | Strictly require the Pub/Sub push envelope and decoded Cloud Logging `LogEntry`. |
| Relay becomes coupled to storage schema internals | Shallow-validate only required v1 notification keys; forward payload unchanged. |
| Relay import accidentally pulls DB clients | Add source-inspection tests rejecting storage/DB imports. |
| WD config is missing and messages churn forever | Validate config at startup so unhealthy revisions do not serve traffic. |
| App deploy does not build the new service image | Add `feed_audit_webhook` to deployment workflow filters, service list, Dockerfile path behavior, and Cloud Run service-name mapping. |

## Planning Recommendation

Use four execution plans:

1. Public relay service scaffold and configuration contract.
2. Pub/Sub `LogEntry` parser and shallow payload validation.
3. WD client plus endpoint ACK/NACK behavior.
4. Deployment Cloud Run service wiring, route ack deadline, and app-deploy
   support.

