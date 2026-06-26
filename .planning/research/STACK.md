# Stack Research

**Domain:** Feed audit notification delivery via Cloud Logging structured logs -> Pub/Sub -> Cloud Run webhook relay
**Researched:** 2026-06-26
**Confidence:** HIGH for application stack and GCP primitives; MEDIUM for final Terraform composition because the private deployment root was not available in this worktree.

## Recommendation

Add a small Python 3.13 Cloud Run service under the existing backend pipeline tree. Feed audit producers should emit a tightly scoped structured log after a feed audit event is committed. Cloud Logging should route only those log entries to a dedicated Pub/Sub topic. A Pub/Sub push subscription should call the Cloud Run relay, and the relay should parse the exported LogEntry JSON, extract the feed audit payload, and POST a normalized webhook payload with bounded retries.

This is intentionally a best-effort delivery path. The durable audit ledger in AlloyDB remains the source of truth. Do not add a database outbox, dispatcher state table, worker queue, or replay UI for this milestone unless the product requirement changes from "best-effort" to "guaranteed delivery".

The requested codebase research files were not present at wrapper paths like `.planning/codebase/STACK.md`; the existing-code context used here is from `radio-transcription/.planning/codebase/STACK.md`, `INTEGRATIONS.md`, and `ARCHITECTURE.md`.

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| Python | 3.13.2, `>=3.13,<3.14` | Relay service runtime | Matches the repo-wide backend runtime, Docker base images, Ruff target, and uv workspace. Avoids introducing another runtime for a narrow backend service. |
| `uv` | 0.9.28 dev pin; Docker currently copies `ghcr.io/astral-sh/uv:0.11.13` | Dependency locking and per-package installs | The monorepo already uses uv workspace packages and `uv.lock`. Add one scoped package and run `uv lock`; do not create a separate requirements stack. |
| FastAPI | Existing lower bound `>=0.110.0` | HTTP Pub/Sub push endpoint and health route | Matches existing internal services and the Cloud Run transcription push service. It is easier to unit test than Functions Framework for a webhook relay. |
| Uvicorn | Existing lower bound `>=0.27.0` | ASGI server in Cloud Run | Existing Cloud Run-style services already run `uvicorn ... --host 0.0.0.0 --port $PORT`. |
| Cloud Logging structured logs | `google-cloud-logging>=3.14.0` via `radio-transcription-common` | Producer-side audit event signal | Existing `setup_logging()` and `extra={"json_fields": ...}` patterns are already used. Context7 and Google docs confirm `json_fields` creates structured JSON payloads. |
| Cloud Logging Log Router sink | GCP managed service | Routes selected feed audit structured logs to Pub/Sub | Official Logging docs support sink filters and Pub/Sub destinations. This avoids application-side Pub/Sub publishing in every producer. |
| Pub/Sub topic and push subscription | GCP managed service | Buffer and deliver routed log entries to relay | Official Cloud Run docs show Pub/Sub push invoking private Cloud Run services with OIDC. Pub/Sub retries and DLQ give enough best-effort protection. |
| Cloud Run v2 service | GCP managed service | Containerized webhook relay | Fits the requested "webhook relay", matches the existing transcription Cloud Run push pattern, and avoids Cloud Functions packaging limitations. |
| Terraform | 1.14.5 local pin, provider currently `>=6.0` | Infrastructure resources | Existing modules are Terraform-based. Provider 7.x is current, but this milestone should not force a provider major upgrade unless the private deployment root is already on 7.x. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `radio-transcription-common` | workspace package | Logging, tracing, env helpers, shared exceptions | Required for `setup_logging()`, trace metadata conventions, and consistency with pipeline services. |
| `urllib3` | Existing root lower bound `>=2.7.0` | Outbound webhook POST client | Use the same `PoolManager` plus `Retry` approach as `backend/pipeline/notification/request_handler.py`. Keep this relay synchronous and small. |
| `pydantic` | Existing lower bound `>=2.10.6` | Request and payload validation | Use for typed Pub/Sub envelope, exported LogEntry subset, and outbound webhook schema. Configure extra fields to ignore because LogEntry can evolve. |
| `google-auth` | Existing common lower bound `>=2.29.0` | Optional local/test token verification helper | Do not require app-level JWT verification in production if Cloud Run IAM auth is configured; Cloud Run performs the invoker check before the container receives the request. |
| `opentelemetry-*` | Existing common lower bounds | Trace continuity and structured logs | Reuse common tracing setup if useful, but do not make tracing correctness a delivery prerequisite. |
| `pytest`, `httpx` | Existing dev deps | FastAPI route tests | Unit test with synthetic Pub/Sub push envelopes containing base64-encoded LogEntry JSON; do not require GCP emulators for this relay. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| `uv lock` | Update `uv.lock` after adding the package | Keep the root workspace lock authoritative. |
| `uv sync --frozen --no-dev --package feed-audit-relay-service` | Docker dependency install pattern | Use the same scoped package install style as existing backend Dockerfiles. |
| `ruff` and `ty` | Formatting/lint/type checks | Add tests narrow enough to run with targeted pytest; avoid broad E2E lanes by repo instruction. |
| `terraform fmt -recursive` / `terraform validate` | Infra sanity | Run only where the deployment root is available. The public repo currently exposes modules, not the full private deployment composition. |
| `git diff --check` | Docs-only whitespace check | Appropriate verification for this research artifact. |

## Service Package Shape

Use a new package, not the existing alert `notification-function`, so feed audit delivery remains separate from evaluated radio alert notifications.

```text
radio-transcription/backend/pipeline/feed_audit_relay/
├── __init__.py
├── Dockerfile
├── main.py          # FastAPI app: POST /, GET /healthz
├── models.py        # Pydantic models for Pub/Sub push envelope, LogEntry subset, webhook payload
├── processor.py     # decode envelope -> validate jsonPayload -> classify -> send
├── request_handler.py
├── settings.py      # frozen dataclass env settings, matching ingestion/storage style
├── pyproject.toml
└── tests/
    ├── test_main.py
    ├── test_processor.py
    └── test_request_handler.py
```

Package name: `feed-audit-relay-service`.

Root `pyproject.toml` should add this workspace package to dependencies beside the other backend packages. The package manifest should stay minimal:

```toml
[project]
name = "feed-audit-relay-service"
version = "0.1.0"
requires-python = ">=3.13, <3.14"

dependencies = [
    "fastapi>=0.110.0",
    "uvicorn>=0.27.0",
    "pydantic>=2.10.6",
    "urllib3>=2.7.0",
    "radio-transcription-common",
]

[tool.setuptools]
packages = []
```

Do not add `requests`; the existing notification package declares it but the implementation already uses `urllib3`. Do not add `google-cloud-pubsub` to the relay package unless it publishes its own secondary messages; Pub/Sub push invokes the HTTP service and no SubscriberClient is needed.

## Structured Log Contract

Producers should emit one structured log per committed feed audit event using the existing standard logging pattern:

```python
logger.info(
    "Feed audit event committed",
    extra={
        "json_fields": {
            "event_type": "feed_audit_event",
            "schema_version": "feed_audit_event.v1",
            "audit_event_id": str(audit_event_id),
            "feed_id": str(feed_id),
            "feed_revision": feed_revision,
            "action": action,
            "actor_id": actor_id,
            "occurred_at": occurred_at.isoformat(),
            "before_values": before_values,
            "after_values": after_values,
        }
    },
)
```

Use `extra={"json_fields": ...}` instead of direct `cloud_logging.Client().logger(...).log_struct(...)` in product code. The direct client API is valid, but the repo already centralizes Cloud Logging setup through `backend/pipeline/common/log_helper.py`; using standard logging keeps tests simple and preserves local console behavior.

The relay should never forward the entire Cloud Logging `LogEntry` to the external webhook. It should extract and validate `jsonPayload`, then emit a stable webhook payload. Include `audit_event_id` and `feed_revision` as idempotency and ordering hints because Cloud Logging/Pub/Sub delivery is at-least-once and does not provide feed-local ordering.

## GCP Infrastructure Primitives

| Resource | Recommended Shape | Notes |
|----------|-------------------|-------|
| `google_pubsub_topic.feed_audit_notifications` | Dedicated topic for routed feed audit logs | Do not reuse pipeline audio topics. This is operational audit delivery, not protobuf claim-check traffic. |
| `google_logging_project_sink.feed_audit_notifications` | Project-level sink with `unique_writer_identity = true` and filter on `jsonPayload.event_type="feed_audit_event"` and `jsonPayload.schema_version="feed_audit_event.v1"` | Cloud Logging docs require granting the sink writer identity permission to publish to the Pub/Sub destination. |
| `google_pubsub_topic_iam_member.logging_sink_publisher` | Grant `roles/pubsub.publisher` on only the dedicated topic to the sink writer identity | Avoid project-wide publisher grants. |
| `google_cloud_run_v2_service.feed_audit_relay` | Python container, private invocation, `timeout = "60s"`, low memory, bounded max instances | The relay is I/O-bound. Start with 256Mi or 512Mi, 1 CPU, concurrency 10, max instances 2-3. |
| `google_service_account.feed_audit_relay_runtime` | Runtime identity for the Cloud Run container | It should need no broad GCP roles unless it reads secrets directly. Prefer Secret Manager-backed env injection in Cloud Run. |
| `google_service_account.feed_audit_pubsub_invoker` | Identity Pub/Sub uses for OIDC push | Grant only `roles/run.invoker` on the relay service. |
| `google_project_service_identity.pubsub_agent` plus IAM | Pub/Sub service agent token creation | Google Cloud Run Pub/Sub tutorial requires `roles/iam.serviceAccountTokenCreator` for the Pub/Sub service agent when push auth is used. |
| `google_pubsub_subscription.feed_audit_relay_push` | Push endpoint `${cloud_run_uri}/`, OIDC token, `ack_deadline_seconds = 60`, retry policy with backoff | Return 2xx only after successful delivery or deliberate nonretryable discard. |
| `google_pubsub_topic.feed_audit_notifications_dlq` | Dead-letter topic | Configure max delivery attempts between 5 and 10. Pub/Sub docs allow 5-100. Grant required Pub/Sub service account roles for DLQ use. |

Recommended Log Router filter:

```text
jsonPayload.event_type="feed_audit_event"
jsonPayload.schema_version="feed_audit_event.v1"
severity>=INFO
```

Do not filter on `resource.type` unless deployment confirms all producers. Feed audit events can originate from FastAPI services and ingestion/runtime paths, so over-constraining resource types risks silent drops.

## Runtime Behavior

The Cloud Run endpoint should be synchronous:

1. Accept the Pub/Sub push JSON envelope.
2. Base64-decode `message.data`.
3. Parse the decoded JSON as a Cloud Logging `LogEntry`.
4. Extract `jsonPayload`.
5. Validate `event_type`, `schema_version`, required audit identifiers, and action.
6. POST the normalized payload to `FEED_AUDIT_WEBHOOK_URL` with `X-Api-Key` or the destination's required auth header.
7. Return `204` when the message is accepted.

Recommended classification:

| Condition | Relay HTTP response to Pub/Sub | Rationale |
|-----------|--------------------------------|-----------|
| Valid payload and destination 2xx | `204` | Acknowledges successful delivery. |
| Bad Pub/Sub envelope or bad JSON from a matching sink | `204` after structured error log | Retrying malformed log messages creates poison loops. Sink filters should prevent these. |
| Destination `400`, `404`, `422` | `204` after structured nonretryable error log | Payload/schema mismatch is unlikely to heal through retry. |
| Destination `401`, `403`, `408`, `429`, `5xx`, timeout, connection error | `500` or raised exception | Secret/config/rate/transient failures may heal; let Pub/Sub retry and eventually DLQ. |

Set outbound timeouts explicitly. A good first default is connect timeout 2 seconds and read/total budget under 10 seconds. Keep the Cloud Run timeout and Pub/Sub ack deadline at 60 seconds so duplicate delivery is rare under normal endpoint latency.

## Installation

```bash
# Add the new package manifest and root workspace dependency manually,
# then update the existing uv lockfile.
cd radio-transcription
uv lock

# Build pattern for the new Dockerfile should mirror existing services.
uv sync --frozen --no-dev --package feed-audit-relay-service
```

The Dockerfile should follow the FastAPI service pattern:

```dockerfile
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV WORKDIR=/app
ENV PYTHONPATH=/app
ENV PORT=8080

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package feed-audit-relay-service

COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/pipeline/feed_audit_relay/ ./backend/pipeline/feed_audit_relay/

CMD exec uvicorn backend.pipeline.feed_audit_relay.main:app --host 0.0.0.0 --port $PORT --workers 1
```

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| Cloud Run FastAPI service | Cloud Functions Gen 2 with Functions Framework | Use only if deployment strongly prefers zip/function packaging. The repo's Cloud Run transcription service already proves FastAPI push is acceptable. |
| Pub/Sub push subscription | Pull subscriber worker | Use pull only if the relay needs long-running batch control. Push is simpler for low-volume webhook relay and aligns with Cloud Run docs. |
| Log Router sink to Pub/Sub | Application publishes Pub/Sub directly | Direct publish is better for stronger producer-side delivery guarantees. For this milestone, a structured log sink keeps producers decoupled and best-effort. |
| `urllib3.PoolManager` | `requests` | `requests` is simpler but adds no value here and is not used by the current notification implementation. |
| `urllib3.PoolManager` | `httpx.AsyncClient` | Use `httpx` only if the relay becomes fully async or calls multiple downstream services. A single outbound POST does not justify it. |
| New `feed_audit_relay` package | Extend `backend/pipeline/notification` | Keep alert notifications and feed audit notifications separate; they have different triggers, payloads, and delivery semantics. |
| Terraform resources in existing modules/deployment root | Pulumi, Cloud Deploy, ad hoc `gcloud` scripts | The repo already uses Terraform modules and private deployment automation. Do not introduce another IaC tool. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| Database outbox table for this milestone | Changes the requirement from best-effort notification to durable dispatch and creates new operational state | Structured log event plus Pub/Sub/DLQ. |
| Redis/Celery/RQ worker | Adds queue infrastructure and runtime ownership not needed for low-volume audit notifications | Pub/Sub retry/DLQ and Cloud Run scaling. |
| Dataflow or Beam | Heavyweight for per-event webhook relay | Single Cloud Run service. |
| Eventarc Pub/Sub trigger for this relay | Produces CloudEvent routing and extra trigger surface; direct Pub/Sub push is already used by the repo's transcription service | Pub/Sub push subscription with OIDC. |
| Public unauthenticated Cloud Run endpoint | Pub/Sub supports authenticated push and Cloud Run has built-in invoker checks | Private Cloud Run service plus Pub/Sub OIDC service account. |
| `requests` dependency | Existing request code uses `urllib3`; adding both makes retry/timeout conventions inconsistent | `urllib3.PoolManager` with explicit `Retry` and timeout. |
| Raw `os.environ` scattered through handlers | Makes config validation inconsistent and hard to test | A small frozen dataclass `RelaySettings`, matching storage/ingestion settings style. |
| Raw LogEntry forwarding | Leaks Cloud Logging metadata and couples external receivers to GCP internals | Extract `jsonPayload` and send a stable v1 webhook schema. |
| Relying on Pub/Sub ordering | Logging export does not provide feed-local ordering keys | Include `audit_event_id` and `feed_revision`; receiver dedupes and sorts if needed. |

## Stack Patterns by Variant

**If delivery stays best-effort:**
- Use Cloud Logging structured logs, Log Router sink, Pub/Sub push, Cloud Run relay, and DLQ.
- Because this keeps the audit write path small and preserves AlloyDB as the durable truth.

**If product later requires guaranteed delivery or replay:**
- Add an explicit database outbox table keyed by `feed_audit_events.id` and a dispatcher that records attempts.
- Because Cloud Logging export is not a transactional outbox and cannot prove every committed audit row was delivered.

**If webhook volume spikes:**
- Increase Cloud Run max instances/concurrency and tune Pub/Sub retry backoff.
- Keep the same service shape before considering Dataflow or worker pools.

**If the destination requires private networking:**
- Add Serverless VPC Access or Private Service Connect only after confirming the endpoint is private.
- Do not add VPC egress for a public HTTPS webhook.

## Version Compatibility

| Package or Platform | Compatible With | Notes |
|---------------------|-----------------|-------|
| Python 3.13.2 | Existing backend package bounds `>=3.13,<3.14` | Use `python:3.13-slim` Docker base like current backend services. |
| FastAPI `>=0.110.0` + Uvicorn `>=0.27.0` | Repo's Cloud Run/FastAPI patterns | Keep one Uvicorn worker. Scale with Cloud Run instances, not local worker fanout. |
| `google-cloud-logging>=3.14.0` | Existing `radio-transcription-common` | Context7 confirmed Python logging supports `extra={"json_fields": ...}` structured JSON. |
| `urllib3>=2.7.0` | Existing root dependency and notification request handler | Use explicit timeout and `Retry(total=3, status_forcelist=[408,429,500,502,503,504])`. |
| Terraform 1.14.5 | Existing `.tool-versions` | Current module constraints allow Google provider `>=6.0`. HashiCorp Google provider 7.38.0 is latest as of 2026-06-23, but 7.x is a major upgrade with breaking-change risk. |
| Cloud Run timeout 60s | Pub/Sub push ack deadline 60s | Google docs say Cloud Run default is 300s and max is 3600s, but this relay should fail fast. |
| Pub/Sub dead-letter policy | 5-100 max delivery attempts | Start with 10 attempts for operational failures, then DLQ. |

## Confidence by Recommendation

| Recommendation | Confidence | Why |
|----------------|------------|-----|
| FastAPI/Uvicorn Cloud Run service | HIGH | Existing repo already uses this pattern for Pub/Sub push transcription and internal services. |
| Structured log emission with `json_fields` | HIGH | Verified in Context7 and official Cloud Logging structured logging docs; repo already uses it. |
| Log Router sink to Pub/Sub | HIGH | Official Cloud Logging docs describe Pub/Sub destinations and required sink writer IAM. |
| Authenticated Pub/Sub push to private Cloud Run | HIGH | Official Cloud Run tutorial includes OIDC push subscription, invoker SA, and token creator IAM. |
| `urllib3` as request client | HIGH | Existing notification implementation already uses it; avoids adding another HTTP convention. |
| Terraform resource names and module split | MEDIUM | Resource primitives are official, but the private deployment root is not visible here. |
| Exact Cloud Run sizing | MEDIUM | Expected volume is low, but production audit event rate and webhook endpoint rate limits were not available. |

## Sources

- Context7 `/googleapis/python-logging` - verified structured log writes and log sink concepts for `google-cloud-logging`.
- Context7 `/googleapis/google-cloud-python` - verified `google-cloud-pubsub` package docs and Python logging `json_fields` example.
- Google Cloud Logging structured logging docs - https://docs.cloud.google.com/logging/docs/structured-logging
- Google Cloud Logging query language docs - https://docs.cloud.google.com/logging/docs/view/logging-query-language
- Google Cloud Logging route logs to destinations docs - https://docs.cloud.google.com/logging/docs/export/configure_export_v2
- Google Cloud Logging Pub/Sub export docs - https://docs.cloud.google.com/logging/docs/export/pubsub
- Google Cloud Pub/Sub push subscriptions docs, last updated 2026-06-18 - https://docs.cloud.google.com/pubsub/docs/push
- Google Cloud Pub/Sub push authentication docs - https://docs.cloud.google.com/pubsub/docs/authenticate-push-subscriptions
- Google Cloud Run Pub/Sub tutorial - https://docs.cloud.google.com/run/docs/tutorials/pubsub
- Google Cloud Run request timeout docs - https://docs.cloud.google.com/run/docs/configuring/request-timeout
- Google Cloud Pub/Sub subscription properties docs - https://docs.cloud.google.com/pubsub/docs/subscription-properties
- HashiCorp Google provider 7.0 GA announcement - https://www.hashicorp.com/en/blog/terraform-provider-for-google-cloud-7-0-is-now-ga
- HashiCorp Terraform Google provider releases, latest observed `v7.38.0` on 2026-06-23 - https://github.com/hashicorp/terraform-provider-google/releases

---
*Stack research for: feed audit notification delivery*
*Researched: 2026-06-26*
