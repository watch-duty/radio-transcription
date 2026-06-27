# Phase 3: Webhook Relay Delivery - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-27
**Phase:** 3-Webhook Relay Delivery
**Areas discussed:** Relay package shape, Pub/Sub envelope validation, WD forwarding and retry behavior, Configuration and secret contract

---

## Relay Package Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated relay service package | Create `backend/pipeline/feed_audit_webhook` with its own package, Dockerfile, FastAPI app, client, parser, and tests. | ✓ |
| Reuse `backend/pipeline/notification` | Add audit webhook delivery beside the existing alert notification function. | |
| You decide | Let the planner choose the simplest long-term structure. | |

**User's choice:** Dedicated relay service package.
**Notes:** User selected `feed_audit_webhook` for naming, FastAPI + Uvicorn for runtime style, and `urllib3` for outbound WD calls.

## Pub/Sub Envelope Validation

| Option | Description | Selected |
|--------|-------------|----------|
| Strict Pub/Sub + LogEntry shape | Require standard push envelope, base64 message data, valid JSON LogEntry, and object `jsonPayload`. | ✓ |
| Tolerant extraction | Accept wrapped messages and direct payloads on the same endpoint. | |
| Separate test endpoint | Keep production strict and add a dev/test-only direct payload endpoint. | |

**User's choice:** Strict Pub/Sub + Cloud Logging LogEntry shape.
**Notes:** User selected shallow v1 contract validation, non-2xx for malformed/unsupported messages, and forwarding the extracted flat `jsonPayload` unchanged.

## WD Forwarding And Retry Behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Retry transient failures only | Retry timeout, connection failure, `408`, `429`, and `5xx`; do not locally retry non-transient `4xx`. | ✓ |
| Retry every non-2xx | Simpler, but wastes local retry on permanent failures. | |
| No local retry | Let Pub/Sub do all retries. | |

**User's choice:** Retry transient failures only.
**Notes:** User asked to compare existing notification and ingestion behavior. Existing alert notification ACKs `4xx`, but ingestion treats `408`, `429`, and `5xx` as transient. User agreed to align the relay with ingestion-style transient classification, use two total WD attempts, return Pub/Sub `204` only after WD `2xx`, use about `15s` per WD attempt, add a tiny `250-500ms` jittered delay between attempts, and log the full WD response body on failures. This creates an ack-deadline coordination item because Phase 2 currently configured a 10 second push ack deadline.

## Configuration And Secret Contract

| Option | Description | Selected |
|--------|-------------|----------|
| Base URL env var + fixed path in code | Use `WD_BACKEND_BASE_URL` and append the fixed WD audit webhook path in code. | ✓ |
| Full webhook URL env var | Configure the whole webhook URL per environment. | |
| You decide | Let the planner choose. | |

**User's choice:** Base URL env var + fixed path in code.
**Notes:** User selected `WD_BACKEND_API_KEY`, startup config validation, hardcoded `event_type`/`schema_version` constants, and the rule that anything reusable belongs in the public repo while deployment-specific wiring and secrets belong in the deployment repo.

## The Agent's Discretion

- Exact module/class/function names are discretionary.
- Exact FastAPI non-2xx status codes for different failure classes are discretionary as long as ACK/NACK semantics are preserved.
- Exact test file layout is discretionary.
- Terraform module placement is discretionary only after evaluating whether it is genuinely reusable without private deployment assumptions.

## Deferred Ideas

- Replay selected audit events from `feed_audit_events`.
- Delivery attempt history outside Cloud Logging/Pub/Sub DLQ.
- Stronger HMAC-style outbound webhook authentication.
- Multi-destination fanout.
- Operational dashboards, staging proof, production rollout runbook, and DLQ inspection guidance.
