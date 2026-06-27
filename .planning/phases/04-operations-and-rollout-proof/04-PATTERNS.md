# Phase 04: Operations and Rollout Proof - Pattern Map

**Mapped:** 2026-06-27
**Files analyzed:** 19 candidate new/modified files
**Analogs found:** 19 / 19

## Scope Notes

- Public repo changes are conditional and should stay limited to reusable relay log/test hardening for `OPS-01`.
- Deployment repo changes own concrete environment route enablement, Terraform monitoring, dashboards, runbooks, and triage docs.
- Do not add durable delivery tables, replay tools, DB polling, CDC, triggers, `LISTEN/NOTIFY`, custom DLQ consumers, or feed write-path proof hooks in v1.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `backend/pipeline/feed_audit_webhook/main.py` | controller | request-response, event-driven | `backend/pipeline/feed_audit_webhook/main.py` | exact |
| `backend/pipeline/feed_audit_webhook/wd_client.py` | service | request-response, retry | `backend/pipeline/feed_audit_webhook/wd_client.py` | exact |
| `backend/pipeline/feed_audit_webhook/tests/test_main.py` | test | request-response | `backend/pipeline/feed_audit_webhook/tests/test_main.py` | exact |
| `backend/pipeline/feed_audit_webhook/tests/test_wd_client.py` | test | request-response, retry | `backend/pipeline/feed_audit_webhook/tests/test_wd_client.py` | exact |
| `backend/pipeline/feed_audit_webhook/tests/test_pubsub.py` | test | transform, request-response | `backend/pipeline/feed_audit_webhook/tests/test_pubsub.py` | exact |
| `../../feed-audit-notification-routing-deployment/terraform/environments/dev/main.tf` | config | resource composition | same file | exact |
| `../../feed-audit-notification-routing-deployment/terraform/environments/prod/main.tf` | config | resource composition | same file | exact |
| `../../feed-audit-notification-routing-deployment/terraform/modules/app/variables.tf` | config | resource composition | same file | exact |
| `../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf` | config | event-driven, monitoring | same file | exact |
| `../../feed-audit-notification-routing-deployment/terraform/modules/app/monitoring.tf` | config | monitoring transform | same file | exact |
| `../../feed-audit-notification-routing-deployment/terraform/modules/app/outputs.tf` | config | resource composition | same file | exact |
| `../../feed-audit-notification-routing-deployment/terraform/modules/app/dashboards/system_health_overview.json.tftpl` | config | monitoring dashboard | same file | exact |
| `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/SKILL.md` | docs | request-response triage | same file | exact |
| `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/console-deep-links.md` | docs | request-response triage | same file | exact |
| `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md` | docs | request-response triage | same file | exact |
| `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/triage-flows/feed-audit-notification.md` | docs | event-driven triage | `triage-flows/perf-regression.md` | role-match |
| `../../feed-audit-notification-routing-deployment/docs/feed-audit-notification-rollout.md` | docs | request-response rollout proof | `docs/dataflow-iam-runbook.md` | role-match |
| `../../feed-audit-notification-routing-deployment/scripts/verify_feed_audit_notification_route.py` (optional) | utility | batch, request-response CLI | `scripts/verify_required_env.py` | role-match |
| `../../feed-audit-notification-routing-deployment/scripts/tests/test_verify_feed_audit_notification_route.py` (optional) | test | batch | `scripts/tests/test_verify_required_env.py` | role-match |

## Pattern Assignments

### `backend/pipeline/feed_audit_webhook/main.py` (controller, request-response/event-driven)

**Analog:** `backend/pipeline/feed_audit_webhook/main.py`

**Imports and app setup pattern** (lines 5-24):
```python
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Protocol

from fastapi import FastAPI, Request, Response, status

from backend.pipeline.common.log_helper import setup_logging
from backend.pipeline.feed_audit_webhook.pubsub import (
    InvalidPubSubMessage,
    extract_feed_audit_payload,
)
```

**Endpoint ACK/NACK pattern** (lines 56-84):
```python
@relay_app.post("/pubsub/feed-audit-notifications")
async def receive_feed_audit_notification(
    envelope: dict[str, Any],
    request: Request,
) -> Response:
    try:
        payload = extract_feed_audit_payload(envelope)
    except InvalidPubSubMessage:
        logger.warning("Invalid Feed Audit Notification Pub/Sub message")
        return Response(status_code=status.HTTP_400_BAD_REQUEST)

    sender: WebhookSender | None = getattr(request.app.state, "wd_client", None)
    if sender is None:
        logger.warning("Feed audit webhook relay WD client is not initialized")
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)

    try:
        await asyncio.to_thread(sender.send, payload)
    except WatchDutyWebhookError:
        return Response(status_code=status.HTTP_502_BAD_GATEWAY)

    return Response(status_code=status.HTTP_204_NO_CONTENT)
```

**Planner guidance:** if `OPS-01` needs better malformed/config logs, keep this endpoint shape and add `extra={"json_fields": ...}` to the two warning paths. Preserve non-2xx returns so Pub/Sub retries and DLQ behavior remain platform-owned.

---

### `backend/pipeline/feed_audit_webhook/wd_client.py` (service, request-response/retry)

**Analog:** `backend/pipeline/feed_audit_webhook/wd_client.py`

**Retry constants and transient exception pattern** (lines 29-40):
```python
_MAX_ATTEMPTS = 2
_REQUEST_TIMEOUT_SECONDS = 15.0
_RETRY_JITTER_MIN_SECONDS = 0.25
_RETRY_JITTER_MAX_SECONDS = 0.5
_RETRYABLE_STATUS_CODES = {408, 429}
_TRANSIENT_EXCEPTIONS = (
    ConnectTimeoutError,
    MaxRetryError,
    NewConnectionError,
    ReadTimeoutError,
    Urllib3TimeoutError,
)
```

**Structured success/failure logging pattern** (lines 148-158, 194-216):
```python
if 200 <= status_code < 300:
    logger.info(
        "Feed Audit Notification delivered to Watch Duty",
        extra={
            "json_fields": _log_fields(
                payload,
                status_code=status_code,
                attempts=attempt,
            )
        },
    )

logger.log(
    log_level,
    "Feed Audit Notification delivery to Watch Duty failed",
    extra={
        "json_fields": _log_fields(
            payload,
            status_code=status_code,
            attempts=attempts,
            retryable=retryable,
            response_body=response_body,
        )
    },
)
```

**Field taxonomy pattern** (lines 229-249):
```python
fields: dict[str, Any] = {
    "relay_event": "feed_audit_webhook_delivery",
    "event_id": payload.get("event_id"),
    "feed_id": payload.get("feed_id"),
    "feed_revision": payload.get("feed_revision"),
    "wd_status_code": status_code,
    "attempts": attempts,
}
if retryable is not None:
    fields["retryable"] = retryable
if response_body is not None:
    fields["wd_response_body"] = response_body
```

**Planner guidance:** reuse `relay_event`, `event_id`, `feed_id`, `feed_revision`, `retryable`, and low-cardinality failure fields. Do not add `before_values`, `after_values`, API keys, arbitrary response-body labels, or event IDs as metric labels.

---

### Relay Tests (test, request-response/retry/transform)

**Files:** `backend/pipeline/feed_audit_webhook/tests/test_main.py`, `test_wd_client.py`, `test_pubsub.py`

**Endpoint test pattern** from `test_main.py` (lines 61-126):
```python
def test_valid_message_and_wd_success_returns_204() -> None:
    payload = _payload()
    wd_client = _FakeWDClient()
    app = create_app(settings=_settings(), wd_client=wd_client)

    with TestClient(app) as client:
        response = client.post(
            "/pubsub/feed-audit-notifications",
            json=_envelope(payload),
        )

    assert response.status_code == 204
    assert wd_client.payloads == [payload]

def test_malformed_pubsub_message_returns_non_2xx_without_calling_wd() -> None:
    wd_client = _FakeWDClient()
    app = create_app(settings=_settings(), wd_client=wd_client)

    with TestClient(app) as client:
        response = client.post("/pubsub/feed-audit-notifications", json={})

    assert response.status_code == 400
    assert wd_client.payloads == []
```

**Retry and sanitization test pattern** from `test_wd_client.py` (lines 88-178):
```python
@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503])
def test_send_retries_transient_statuses_once(status_code: int) -> None:
    client, http, sleeps = _client(
        [_Response(status=status_code, data=b"try again"), _Response(status=204)]
    )
    result = client.send(_payload())
    assert result.attempts == 2
    assert sleeps == [0.25]

def test_failure_logs_response_body_without_api_key(caplog: pytest.LogCaptureFixture) -> None:
    api_key = "very-secret-key"
    client, _http, _sleeps = _client([_Response(status=401, data=b"not authorized")], api_key=api_key)
    with caplog.at_level(logging.ERROR, logger=wd_client.__name__):
        with pytest.raises(WatchDutyWebhookError):
            client.send(_payload())
    assert api_key not in caplog.text
    fields = [getattr(record, "json_fields", {}) for record in caplog.records]
    assert all("before_values" not in field for field in fields)
    assert all("after_values" not in field for field in fields)
```

**Parser validation test pattern** from `test_pubsub.py` (lines 70-108):
```python
@pytest.mark.parametrize(
    "envelope",
    [{}, {"message": {}}, {"message": {"data": "not base64"}}, _invalid_json_envelope()],
)
def test_extract_feed_audit_payload_rejects_malformed_envelopes(envelope: dict[str, Any]) -> None:
    with pytest.raises(InvalidPubSubMessage):
        extract_feed_audit_payload(envelope)

@pytest.mark.parametrize(
    "payload",
    [
        _feed_audit_payload(event_type="other.event"),
        _feed_audit_payload(schema_version=2),
        _feed_audit_payload(before_values=[]),
        _feed_audit_payload(after_values=[]),
    ],
)
def test_extract_feed_audit_payload_rejects_unsupported_payloads(payload: dict[str, object]) -> None:
    with pytest.raises(InvalidPubSubMessage):
        extract_feed_audit_payload(_pubsub_envelope({"jsonPayload": payload}))
```

**Planner guidance:** add `caplog` assertions for new structured warning fields if `main.py` logging changes. Keep tests focused; do not add integration tests or Docker/testcontainers for this phase.

---

### `../../feed-audit-notification-routing-deployment/terraform/environments/dev/main.tf` and `prod/main.tf` (config, resource composition)

**Analog:** existing environment roots.

**Dev app module argument style** (lines 1-40):
```hcl
module "app" {
  source = "../../modules/app"

  project_id                      = var.project_id
  region                          = var.region
  environment                     = var.environment
  allow_production_deployment     = var.allow_production_deployment
  wd_backend_endpoint             = var.wd_backend_endpoint
  wd_backend_endpoint_api_key     = var.wd_backend_endpoint_api_key
  ingestion_staging_bucket_name   = var.ingestion_staging_bucket_name
  ingestion_canonical_bucket_name = var.ingestion_canonical_bucket_name
  ...
  evaluation_max_instances = 1
}
```

**Prod notification-channel threading style** (lines 40-86):
```hcl
module "app" {
  source = "../../modules/app"

  project_id                  = var.project_id
  region                      = var.region
  environment                 = var.environment
  wd_backend_endpoint         = var.wd_backend_endpoint
  wd_backend_endpoint_api_key = var.wd_backend_endpoint_api_key
  ...
  enable_monitoring = true

  slack_critical_notification_channel_id = google_monitoring_notification_channel.slack_critical.id
}
```

**Planner guidance:** if Phase 4 enables the route, pass `feed_audit_notification_route_enabled` explicitly in env roots. Dev/staging proof should happen before any prod enablement.

---

### `../../feed-audit-notification-routing-deployment/terraform/modules/app/variables.tf` (config)

**Analog:** same file.

**Feature flag and relay URL variable pattern** (lines 121-135):
```hcl
variable "feed_audit_notification_route_enabled" {
  description = "Whether to enable Cloud Logging to Pub/Sub routing for Feed Audit Notification logs."
  type        = bool
  default     = false
}

variable "feed_audit_webhook_wd_backend_base_url" {
  description = "Optional Watch Duty backend base URL for the Feed Audit Webhook relay. Defaults to the origin of wd_backend_endpoint."
  type        = string
  default     = null

  validation {
    condition     = var.feed_audit_webhook_wd_backend_base_url == null ? true : can(regex("^https?://[^/?#]+$", var.feed_audit_webhook_wd_backend_base_url))
    error_message = "feed_audit_webhook_wd_backend_base_url must be null or an absolute HTTP(S) origin without a trailing slash or path."
  }
}
```

**Monitoring channel variable pattern** (lines 178-188):
```hcl
variable "enable_monitoring" {
  description = "Forwarded to the ingestion module's monitoring sub-module. Set to true in prod only."
  type        = bool
  default     = false
}

variable "slack_critical_notification_channel_id" {
  description = "Forwarded to the ingestion module. Full GCP resource name of the critical-tier Slack notification channel..."
  type        = string
  default     = null
}
```

**Planner guidance:** add new monitoring toggles only if needed. Keep environment-specific URLs/secrets as variables or env-root wiring, not public repo constants.

---

### `../../feed-audit-notification-routing-deployment/terraform/modules/app/main.tf` (config, event-driven/monitoring)

**Analog:** same file.

**Feed audit route module composition** (lines 111-137):
```hcl
module "message_queues" {
  source      = "../message_queues"
  depends_on  = [google_project_service.apis]
  environment = var.environment
}

module "feed_audit_notification_route" {
  source = "../feed_audit_notification_route"
  count  = var.feed_audit_notification_route_enabled ? 1 : 0

  project_id                     = var.project_id
  region                         = var.region
  environment                    = var.environment
  notification_topic_id          = module.message_queues.topic_feed_audit_notification_id
  notification_topic_name        = module.message_queues.topic_feed_audit_notification_name
  dead_letter_topic_id           = module.message_queues.topic_feed_audit_notification_dlq_id
  dead_letter_topic_name         = module.message_queues.topic_feed_audit_notification_dlq_name
  relay_service_url              = module.feed_audit_webhook.feed_audit_webhook_service_url
  relay_service_name             = module.feed_audit_webhook.feed_audit_webhook_service_name
  deployer_service_account_email = local.deployer_sa_email
}
```

**Feed audit relay service module** (lines 263-273):
```hcl
module "feed_audit_webhook" {
  source = "../services/feed_audit_webhook"

  project_id          = var.project_id
  region              = var.region
  environment         = var.environment
  wd_backend_base_url = local.feed_audit_webhook_wd_backend_base_url
  wd_backend_api_key  = var.wd_backend_endpoint_api_key

  depends_on = [google_project_service.apis]
}
```

**Alert policy style in this file** (lines 548-610):
```hcl
resource "google_monitoring_alert_policy" "dataflow_input_backpressure_alert" {
  for_each = toset([
    "continuous-audio-sub-${var.environment}",
    "segmented-audio-sub-${var.environment}"
  ])

  display_name = "Dataflow Input Stream Backpressure - ${each.value} (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.slack_critical_notification_channel_id != null ? [var.slack_critical_notification_channel_id] : []
  ...
  user_labels = {
    severity  = "warning"
    subsystem = "dataflow"
  }
}
```

**Planner guidance:** use the existing app module composition and alert style. If adding feed-audit route/DLQ alert policies in `main.tf`, keep conditions on platform metrics and route notification channels through the existing nullable channel pattern.

---

### `../../feed-audit-notification-routing-deployment/terraform/modules/app/monitoring.tf` (config, monitoring transform)

**Analog:** same file.

**Log-based metric pattern** (lines 8-44):
```hcl
resource "google_logging_metric" "transcription_e2e_latency_ms" {
  project     = var.project_id
  name        = "transcription_e2e_latency_ms"
  description = "End-to-end processing latency from ingestion to evaluation."
  filter      = <<-EOT
    resource.type="cloud_run_revision"
    AND jsonPayload.event_type="e2e_latency"
    AND jsonPayload.latency_ms >= 0
  EOT

  value_extractor = "EXTRACT(jsonPayload.latency_ms)"

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "ms"
  }

  lifecycle {
    create_before_destroy = true
  }
}
```

**Metric propagation delay + alert pattern** (lines 46-100):
```hcl
resource "time_sleep" "wait_for_latency_metric" {
  depends_on = [
    google_logging_metric.transcription_e2e_latency_ms,
  ]
  create_duration = "120s"
}

resource "google_monitoring_alert_policy" "transcription_e2e_latency" {
  project      = var.project_id
  display_name = "E2E Latency P95 > 5m (${var.environment})"
  combiner     = "OR"
  enabled      = true

  notification_channels = var.slack_critical_notification_channel_id != null ? [var.slack_critical_notification_channel_id] : []
  depends_on = [time_sleep.wait_for_latency_metric]
}
```

**Planner guidance:** if adding log-based feed audit relay metrics, use low-cardinality filters such as `jsonPayload.relay_event`, `jsonPayload.retryable`, or `failure_class`. Add `time_sleep` before policies that reference new metrics.

---

### `../../feed-audit-notification-routing-deployment/terraform/modules/app/outputs.tf` (config)

**Analog:** same file.

**Route and relay output pattern** (lines 57-84):
```hcl
output "feed_audit_notification_logging_sink_name" {
  description = "The Feed Audit Notification Cloud Logging sink name, or null when disabled."
  value       = try(module.feed_audit_notification_route[0].logging_sink_name, null)
}

output "feed_audit_notification_subscription_name" {
  description = "The Feed Audit Notification Pub/Sub push subscription name, or null when disabled."
  value       = try(module.feed_audit_notification_route[0].feed_audit_notification_subscription_name, null)
}

output "feed_audit_webhook_service_url" {
  description = "The Feed Audit Webhook relay Cloud Run service URL."
  value       = module.feed_audit_webhook.feed_audit_webhook_service_url
}
```

**Planner guidance:** add outputs only for operator/runbook values that are useful and safe. Keep secret values out of outputs.

---

### `../../feed-audit-notification-routing-deployment/terraform/modules/app/dashboards/system_health_overview.json.tftpl` (config, dashboard)

**Analog:** same file.

**Pub/Sub throughput/backlog panel pattern** (lines 585-657):
```json
{
  "legendTemplate": "6. Triggered Notifications (sent-messages)",
  "timeSeriesQuery": {
    "timeSeriesFilter": {
      "aggregation": {
        "alignmentPeriod": "60s",
        "crossSeriesReducer": "REDUCE_SUM",
        "perSeriesAligner": "ALIGN_SUM"
      },
      "filter": "metric.type=\"pubsub.googleapis.com/subscription/sent_message_count\" resource.type=\"pubsub_subscription\" resource.labels.subscription_id=\"alert-notification-subscription-${environment}\""
    }
  }
}
```

**DLQ panel pattern** (lines 941-1077):
```json
{
  "widget": {
    "text": {
      "content": "# Dead Letter Queues (DLQ)\nMetrics monitoring processing failures and message drop-offs at each pipeline stage.",
      "format": "MARKDOWN"
    },
    "title": "Section: Dead Letter Queues (DLQ)"
  }
},
{
  "widget": {
    "title": "DLQ Publish Rates (Failures/sec)",
    "xyChart": {
      "dataSets": [
        {
          "legendTemplate": "Evaluated Audio DLQ / Notification Failures",
          "timeSeriesQuery": {
            "timeSeriesFilter": {
              "filter": "metric.type=\"pubsub.googleapis.com/topic/send_request_count\" resource.type=\"pubsub_topic\" resource.labels.topic_id=\"evaluated-audio-dead-letter-${environment}\""
            }
          }
        }
      ]
    }
  }
}
```

**Dashboard Terraform wrapper** from `dashboards.tf` (lines 3-7):
```hcl
resource "google_monitoring_dashboard" "system_health_overview" {
  dashboard_json = templatefile("${path.module}/dashboards/system_health_overview.json.tftpl", {
    environment = var.environment
  })
}
```

**Planner guidance:** if dashboard panels are needed, copy existing JSON structure and add feed audit topic/subscription/DLQ series. Keep `${environment}` templating and verify generated dev JSON through the existing dashboard workflow.

---

### Pipeline Triage Docs (docs, request-response/event-driven triage)

**Files:** `SKILL.md`, `console-deep-links.md`, `alert-policies.md`, `triage-flows/feed-audit-notification.md`

**Source-of-truth rule** from `SKILL.md` (lines 35-40):
```markdown
This skill never treats its markdown as the source of truth for configurable values. Terraform and Python source are canonical for alert thresholds, SLO targets, channel names, project IDs, metric filters, service names, and API contracts.

Before answering with an exact threshold, route, service name, metric filter, or endpoint, open the cited Terraform/Python source or read the live GCP resource.
```

**Workflow index pattern** from `SKILL.md` (lines 108-116):
```markdown
### 5. Alert policy management

The alert-policy source index lives in `alert-policies.md`. It links to Terraform resources and triage flows; Terraform and live GCP hold the actual thresholds, filters, display names, and notification channels.

Common operations and where they're documented:
- **What does this alert mean?** -> use `alert-policies.md` to map the live policy to Terraform and the relevant triage flow
- **Mute alert X for 1 hour** -> mute playbook in `alert-policies.md` (must include expiry + reason)
- **Add a new alert** -> terraform PR pattern in `alert-policies.md`
```

**Console link placeholder pattern** from `console-deep-links.md` (lines 7-18, 135-164):
```markdown
Placeholders used below:

- `{project}` - GCP project ID
- `{region}` - GCP region (default `us-central1`)
- `{env}` - environment name (`prod` or `dev`)
- `{name}` - resource-specific name (instance group, service, topic, etc.)

Discover live names with `gcloud ... list --filter='name~ingestion' --format=value\(name\)` rather than hardcoding - names live in terraform and are tunable.

### DLQ topic

https://console.cloud.google.com/cloudpubsub/topic/detail/transcribed-audio-dlq-{env}?project={project}
```

**Alert source-index pattern** from `alert-policies.md` (lines 1-13, 74-95):
```markdown
# Alert Policies

Terraform and live GCP policies are the source of truth. This file is only a navigation index; do not copy thresholds, display names, filters, or notification-channel state from here into an answer without checking source or GCP first.

| Area | Terraform source | Resources to inspect | Triage |
|---|---|---|---|
| Transcription/Dataflow alerts | `terraform/modules/app/main.tf` | `dataflow_lag_alert`, `dlq_spike_alert`, `dataflow_input_backpressure_alert` | `triage-flows/perf-regression.md#dataflow-lag`, `#dlq-spike`, `#input-backpressure` |

## How to add a new alert policy

Terraform is the source of truth - never click-create in the GCM console for a permanent alert.
```

**Triage flow procedure pattern** from `triage-flows/perf-regression.md` (lines 142-165):
````markdown
## DLQ spike {#dlq-spike}

**Alert family:** DLQ volume spike. Verify exact topic, threshold, duration, and routing in `terraform/modules/app/main.tf`.

**Procedure:**

1. Open the DLQ topic dashboard. Check the message rate trend.

2. Pull a sample of DLQ messages to see why they failed:

   ```bash
   gcloud pubsub subscriptions pull <dlq-subscription> \
       --auto-ack --limit=10 --project="$PROJECT"
   ```

3. The DLQ message body and attributes tell you which step failed.
````

**Planner guidance:** add feed-audit notification triage docs as navigation/procedure, not as mutable threshold truth. Include producer log query, sink/topic/subscription checks, Cloud Run relay logs, WD response logs, and DLQ pull inspection.

---

### `../../feed-audit-notification-routing-deployment/docs/feed-audit-notification-rollout.md` (docs, rollout proof)

**Analog:** `docs/dataflow-iam-runbook.md` and deployment `README.md`.

**Runbook structure pattern** from `docs/dataflow-iam-runbook.md` (lines 1-20, 138-151):
```markdown
# Dataflow Flex Template - IAM & Configuration Runbook

When using Terraform to manage a GCP project, **`gcloud services enable dataflow`** (or
`google_project_service`) does NOT automatically grant the Dataflow Service Agent its
required IAM role. This is a known gap in the Terraform GCP provider. Every permission
below must be added explicitly.

## Checklist for a New Project

- [ ] `roles/dataflow.serviceAgent` granted to service agent at project level
- [ ] `roles/storage.objectAdmin` granted to service agent on custom staging bucket
- [ ] `roles/iam.serviceAccountUser` granted to service agent on worker SA
```

**Deployment workflow pattern** from `README.md` (lines 36-58):
```markdown
## Deployment Workflow

To ensure stability, this repository follows an automated verification process followed by a manual-apply for production.

1. **Local Checks**: Before committing, run `mise run check`. This handles formatting and linting across the entire repository.
2. **Pull Request**: Opening a PR triggers a `terraform plan` to verify your changes.
3. **Merge to Main**: Merging into `main` triggers a final `terraform plan`.

### Manual Prod Push

Before deploying to production, please check the following:
1. The latest App and Infrastructure Deployments successfully ran.
...
```

**Planner guidance:** the rollout doc should prove a real feed audit row end-to-end, include staging-first/prod rollout gates, and point exact resource names/thresholds back to Terraform or live outputs.

---

### Optional Verification Script and Tests (utility/test, batch/request-response)

**Files:** `scripts/verify_feed_audit_notification_route.py`, `scripts/tests/test_verify_feed_audit_notification_route.py`
**Analog:** `scripts/verify_required_env.py`, `scripts/tests/test_verify_required_env.py`

**Script CLI and subprocess pattern** from `verify_required_env.py` (lines 35-41, 486-523):
```python
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("required_json", type=Path)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--plan", type=Path, metavar="PATH")
    source.add_argument("--template", type=str, metavar="NAME")
    args = parser.parse_args()

    with args.required_json.open(encoding="utf-8") as f:
        required: dict[str, list[str]] = json.load(f)
```

**gcloud call pattern** from `verify_required_env.py` (lines 380-398):
```python
cmd = [
    "gcloud",
    "run",
    "services",
    "describe",
    service,
    "--region",
    region,
    "--project",
    project,
    "--format=json",
]
result = subprocess.run(cmd, capture_output=True, text=True, check=False)
if result.returncode != 0:
    msg = f"gcloud run services describe failed for {service} (rc={result.returncode}): {result.stderr.strip()}"
    raise RuntimeError(msg)
```

**Test pattern** from `test_verify_required_env.py` (lines 631-656, 979-1007):
```python
def test_cloud_run_env_keys_from_plan_happy_path() -> None:
    from scripts.verify_required_env import cloud_run_env_keys_from_plan

    addr = SERVICE_TO_RESOURCE["echo"].address
    plan = _wrap_in_module(
        "module.app.module.ingestion",
        _make_cloud_run_resource(addr, [{"name": "AUDIO_STAGING_BUCKET", "value": "ingestion-staging-bucket-dev"}]),
    )
    assert cloud_run_env_keys_from_plan(plan, addr) == {"AUDIO_STAGING_BUCKET"}

def test_cloud_run_env_keys_from_live_service_raises_on_nonzero_gcloud_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    stderr_text = "ERROR: (gcloud.run.services.describe) NOT_FOUND: service not found"
    ...
    with pytest.raises(RuntimeError) as exc_info:
        vre.cloud_run_env_keys_from_live_service(
            service="nonexistent-service",
            region="us-central1",
            project="watchduty-dev",
        )
    assert "NOT_FOUND" in str(exc_info.value)
```

**Planner guidance:** prefer a manual runbook unless a helper removes real operator error. If a helper is added, keep it read-only, environment-parameterized, and covered by unit tests with mocked subprocesses. Do not create replay or message injection tooling.

## Shared Patterns

### Feed Audit Contract

**Source:** `backend/pipeline/common/feed_audit_notification_contract.py` lines 8-24
**Apply to:** relay logs, runbook queries, route filters, tests

```python
FEED_AUDIT_NOTIFICATION_EVENT_TYPE = (
    "radio_transcription.feed_audit_notification"
)
FEED_AUDIT_NOTIFICATION_SCHEMA_VERSION = 1
FEED_AUDIT_NOTIFICATION_REQUIRED_FIELDS = frozenset(
    {
        "event_type",
        "schema_version",
        "event_id",
        "action",
        "occurred_at",
        "actor_id",
        "feed_id",
        "feed_revision",
        "before_values",
        "after_values",
    }
)
```

### Producer Structured Log, No Network Coupling

**Source:** `backend/pipeline/storage/feed_audit_notifications.py` lines 17-34
**Apply to:** producer proof queries and operational docs

```python
def emit_feed_audit_notification(feed_audit_event: object | None) -> None:
    """Emit a Feed Audit Notification structured log. Never raises."""
    if feed_audit_event is None:
        return

    try:
        payload = _normalize_feed_audit_event(feed_audit_event)
        if payload is None:
            return

        logger.info(
            "Feed audit notification emitted",
            extra={"json_fields": payload},
        )
    except Exception:  # noqa: S110
        pass
```

### No Database Coupling In Relay

**Source:** `backend/pipeline/feed_audit_webhook/README.md` lines 40-44 and `tests/test_no_db_coupling.py` lines 6-21
**Apply to:** all relay code changes

```markdown
The relay does not read or write AlloyDB. It does not poll `feed_audit_events`,
does not create delivery state, and does not import storage-layer SQL or feed
store modules. `feed_audit_events` remains the canonical durable audit ledger.
```

```python
forbidden = (
    "backend.pipeline.storage",
    "asyncpg",
    "psycopg",
    "AlloyDB",
    "connection_pool",
)

for value in forbidden:
    assert value not in source
```

### Cloud Logging Sink To Pub/Sub Route

**Source:** `../../feed-audit-notification-routing-deployment/terraform/modules/feed_audit_notification_route/main.tf` lines 5-24, 77-110
**Apply to:** route enablement, runbook, dashboard, alerts

```hcl
resource "google_logging_project_sink" "feed_audit_notification" {
  project = var.project_id
  name    = "feed-audit-notification-route-${var.environment}"

  destination = "pubsub.googleapis.com/${var.notification_topic_id}"
  filter      = <<-EOT
    jsonPayload.event_type="radio_transcription.feed_audit_notification"
    AND jsonPayload.schema_version=1
  EOT

  unique_writer_identity = true
}

resource "google_pubsub_subscription" "feed_audit_notification_push" {
  name  = "feed-audit-notification-subscription-${var.environment}"
  topic = var.notification_topic_id

  push_config {
    push_endpoint = local.relay_push_endpoint
    oidc_token {
      service_account_email = google_service_account.push_invoker.email
      audience              = var.relay_service_url
    }
  }

  ack_deadline_seconds = 60
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }
  dead_letter_policy {
    dead_letter_topic     = var.dead_letter_topic_id
    max_delivery_attempts = 10
  }
}
```

### DLQ Topic And Pull-Inspection Pattern

**Source:** `../../feed-audit-notification-routing-deployment/terraform/modules/message_queues/main.tf` lines 211-222 and `triage-flows/perf-regression.md` lines 148-157
**Apply to:** rollout proof and DLQ runbook

```hcl
resource "google_pubsub_topic" "feed_audit_notification_dlq" {
  name = "feed-audit-notification-dlq-${var.environment}"
}

resource "google_pubsub_subscription" "feed_audit_notification_dlq_subscription" {
  name  = "feed-audit-notification-dlq-subscription-${var.environment}"
  topic = google_pubsub_topic.feed_audit_notification_dlq.name

  message_retention_duration = "604800s" # 7 days
}
```

```bash
gcloud pubsub subscriptions pull <dlq-subscription> \
    --auto-ack --limit=10 --project="$PROJECT"
```

### Terraform Is Operational Source Of Truth

**Source:** `../../feed-audit-notification-routing-deployment/.claude/skills/pipeline-triage/alert-policies.md` lines 1-3
**Apply to:** docs, alert plans, runbooks

```markdown
Terraform and live GCP policies are the source of truth. This file is only a navigation index; do not copy thresholds, display names, filters, or notification-channel state from here into an answer without checking source or GCP first.
```

## No Analog Found

None. All candidate files have an exact or role-match analog. The optional verification script has only a role-match analog, not a feed-audit-specific route-proof script; planner should keep it optional and prefer runbook-first if a script would add unnecessary surface area.

## Metadata

**Analog search scope:** `backend/pipeline/feed_audit_webhook`, `backend/pipeline/storage`, `backend/services/feeds`, deployment `terraform/`, deployment `.claude/skills/pipeline-triage/`, deployment `docs/`, deployment `scripts/`
**Files scanned:** 125 candidate files by `rg --files`/`find` in targeted public and deployment repo scopes
**Pattern extraction date:** 2026-06-27
**Local project instructions read:** `AGENTS.md`, `.agents/instructions.md`
**Local skill directories:** no project-local `.codex/skills/` or `.agents/skills/` directories found
