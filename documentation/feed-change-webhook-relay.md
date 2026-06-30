# Feed Change Webhook Relay

The Feed Change Webhook relay is a stateless Cloud Run HTTP service that receives
Feed Change Notification log entries from Pub/Sub push delivery and forwards the
flat audit payload to Watch Duty.

## Endpoint

Pub/Sub calls:

```text
POST /pubsub/feed-change-notifications
```

The request body must be the standard Pub/Sub push envelope whose
`message.data` field contains a base64-encoded Cloud Logging `LogEntry`.

## Watch Duty Destination

The relay posts to:

```text
${WD_BACKEND_BASE_URL}/api/v1/echo/radio_transcription/internal/audit/webhook/
```

Required environment variables:

- `WD_BACKEND_BASE_URL`: absolute `http://` or `https://` base URL with no
  trailing slash.
- `WD_BACKEND_API_KEY`: value sent as the `X-Api-Key` header.

## ACK/NACK Contract

The relay returns HTTP `204` to Pub/Sub after Watch Duty returns a `2xx`
response.

Invalid Pub/Sub envelopes, Cloud Logging entries, or Feed Change Notification
payloads are treated as unrecoverable poison input. The relay logs concise
diagnostics and returns HTTP `204` without calling Watch Duty so Pub/Sub does
not retry malformed messages.

Missing runtime configuration, Watch Duty auth/config failures, transient Watch
Duty failures, and unexpected delivery exceptions return non-2xx so Pub/Sub can
retry according to the subscription policy.

## Storage Boundary

The relay does not read or write AlloyDB. It does not poll `feed_audit_events`,
does not create delivery state, and does not import storage-layer SQL or feed
store modules. `feed_audit_events` remains the canonical durable audit ledger.
