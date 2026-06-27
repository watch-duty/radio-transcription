# Phase 3: Webhook Relay Delivery - Pattern Map

**Mapped:** 2026-06-27
**Files analyzed:** public relay analogs and Phase 2 deployment route modules

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `backend/pipeline/feed_audit_webhook/main.py` | service entrypoint | Pub/Sub push -> HTTP webhook | `backend/pipeline/transcription/main.py` | role-match |
| `backend/pipeline/feed_audit_webhook/pubsub.py` | parser/validator | Pub/Sub envelope -> LogEntry `jsonPayload` | `backend/pipeline/storage/feed_audit_notifications.py` + `backend/pipeline/common/tracing_utils.py` | role-match |
| `backend/pipeline/feed_audit_webhook/wd_client.py` | HTTP client | JSON payload -> WD backend | `backend/pipeline/notification/request_handler.py` | role-match |
| `backend/pipeline/feed_audit_webhook/settings.py` | config | env -> service config | `backend/pipeline/notification/send_notification.py` container env properties | partial |
| `backend/pipeline/feed_audit_webhook/pyproject.toml` | package config | workspace dependency | `backend/pipeline/transcription/pyproject.toml` | exact |
| `backend/pipeline/feed_audit_webhook/Dockerfile` | service image | source -> Cloud Run container | `backend/pipeline/transcription/Dockerfile` | exact |
| `pyproject.toml` | workspace config | package membership | existing `[tool.uv.workspace]` members | exact |
| `radio-transcription-deployment/terraform/modules/services/feed_audit_webhook/*` | deployment config | Cloud Run service + secret + IAM | `terraform/modules/services/transcription/*` and `terraform/modules/services/notification/*` | role-match |
| `radio-transcription-deployment/terraform/modules/feed_audit_notification_route/main.tf` | deployment config | Pub/Sub push delivery | existing Phase 2 route module | exact |
| `radio-transcription-deployment/terraform/modules/app/*` | deployment config | module wiring | `terraform/modules/app/main.tf`, `variables.tf`, `outputs.tf` | exact |
| `radio-transcription-deployment/.github/workflows/app_deploy.yml` | CI/CD | image build/deploy | existing Cloud Run service matrix logic | exact |

## Public Repo Patterns

### FastAPI Pub/Sub Push Entrypoint

**Analog:** `backend/pipeline/transcription/main.py`

Pattern:
- call `setup_logging()` once at module import
- define a FastAPI app
- accept a Pub/Sub push JSON envelope as a Python dict
- return `Response(status_code=204)` on successful processing
- use `HTTPException` or non-2xx response to let Pub/Sub retry

Apply:
- create `app = FastAPI(title="Feed Audit Webhook Relay")`
- expose `POST /pubsub/feed-audit-notifications`
- parse and validate the envelope before calling WD
- return `204` only when WD returns a `2xx`

### Producer Payload Constants

**Analog:** `backend/pipeline/storage/feed_audit_notifications.py`

Pattern:
- event type and schema version are code constants
- required key checks are shallow and schema-version based
- validation avoids action-specific audit snapshot knowledge

Apply:
- relay validation must require the same v1 keys
- do not duplicate storage snapshot allowlist fields inside the relay
- if importing constants from storage creates an undesirable dependency, mirror
  constants in `pubsub.py` and add contract tests to keep them aligned

### Outbound WD Webhook Client

**Analog:** `backend/pipeline/notification/request_handler.py`

Pattern:
- `urllib3.PoolManager`
- JSON body
- `Content-Type: application/json`
- `X-Api-Key`
- response body logged for failure triage

Apply:
- keep path fixed as
  `/api/v1/echo/radio_transcription/internal/audit/webhook/`
- configure only `WD_BACKEND_BASE_URL` and `WD_BACKEND_API_KEY`
- do not use `urllib3.Retry` for Phase 3; implement the exact two-attempt
  policy in code so tests can prove the ACK/NACK contract

### Package And Dockerfile

**Analogs:** `backend/pipeline/transcription/pyproject.toml`,
`backend/pipeline/transcription/Dockerfile`

Pattern:
- package-scoped `pyproject.toml`
- service added to root `tool.uv.workspace.members` and dependencies when needed
- Dockerfile copies workspace lock files and package pyprojects, installs the
  target package with `uv export --package ... --locked`, and starts Uvicorn

Apply:
- add `feed-audit-webhook` as a workspace member/package
- include `fastapi`, `uvicorn[standard]`, `urllib3`, and
  `radio-transcription-common`
- start `backend.pipeline.feed_audit_webhook.main:app`

## Deployment Repo Patterns

### Cloud Run Service Module

**Analogs:** `terraform/modules/services/transcription/main.tf`,
`terraform/modules/services/notification/main.tf`

Pattern:
- `google_cloud_run_v2_service` with placeholder image managed by CI/CD
- dedicated service account
- `lifecycle.ignore_changes = [template[0].containers[0].image]`
- standard logging/monitoring/trace roles
- Secret Manager env values where credentials are needed
- output service name and URI

Apply:
- add `terraform/modules/services/feed_audit_webhook`
- service name: `feed-audit-webhook-${var.environment}`
- service account: `feed-audit-webhook-${var.environment}`
- env:
  - `WD_BACKEND_BASE_URL`
  - `WD_BACKEND_API_KEY` from Secret Manager
  - `IS_GCP=true`
  - `GOOGLE_CLOUD_PROJECT`
- no AlloyDB, Redis, VPC connector, Pub/Sub client role, or storage role

### Phase 2 Route Module

**Analog:** `terraform/modules/feed_audit_notification_route/main.tf`

Pattern:
- route module already owns Log Router sink, Pub/Sub push subscription, push
  invoker identity, Cloud Run invoker binding, retry, and DLQ
- `relay_service_url` and `relay_service_name` are inputs
- `push_endpoint` appends `/pubsub/feed-audit-notifications`
- OIDC `audience` is the base relay service URL

Apply:
- keep this route module as the Pub/Sub push/IAM owner
- wire relay URL/name from the new service module
- update `ack_deadline_seconds` from 10 to 60 to match Phase 3 retry budget

### App Module Wiring

**Analog:** `terraform/modules/app/main.tf`

Pattern:
- services are instantiated in the app module, then dependent modules consume
  their outputs
- environment roots pass app-level variables down

Apply:
- instantiate `module "feed_audit_webhook"` before
  `module "feed_audit_notification_route"`
- pass `module.feed_audit_webhook.feed_audit_webhook_service_url` and
  `.feed_audit_webhook_service_name` into the route module
- remove or stop requiring manual relay URL/name app variables if the app module
  can derive them from the service module

### App Deploy Workflow

**Analog:** `.github/workflows/app_deploy.yml`

Pattern:
- service list is an explicit JSON array
- `specific_service` choices are explicit
- `dorny/paths-filter` maps public paths to service names
- Dockerfile path defaults to `public-source/backend/pipeline/${service}`
- Cloud Run service-name mapping has special cases for services whose Cloud Run
  name does not follow `${service}-pipeline-${environment}`

Apply:
- add `feed_audit_webhook` to the service list and `specific_service` choices
- add path filter `backend/pipeline/feed_audit_webhook/**`
- add special Cloud Run service-name mapping to
  `feed-audit-webhook-${environment}`
- include the new service module path in deploy-path force detection if needed

