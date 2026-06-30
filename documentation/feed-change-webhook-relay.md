# Feed Change Webhook Relay

The Feed Change Webhook relay is a stateless Cloud Run HTTP service that receives
Feed Change Notification log entries from Pub/Sub push delivery and forwards the
flat audit payload to a configured destination webhook.

## Endpoint

Pub/Sub calls:

```text
POST /pubsub/feed-change-notifications
```

The request body must be the standard Pub/Sub push envelope whose
`message.data` field contains a base64-encoded Cloud Logging `LogEntry`.

## Destination Webhook

The relay posts to:

```text
${FEED_CHANGE_WEBHOOK_URL}
```

Required environment variables:

- `FEED_CHANGE_WEBHOOK_URL`: absolute `http://` or `https://` destination URL.
- `FEED_CHANGE_WEBHOOK_API_KEY`: value sent as the `X-Api-Key` header.

## ACK/NACK Contract

The relay returns HTTP `204` to Pub/Sub after the destination returns a `2xx`
response.

Invalid Pub/Sub envelopes, Cloud Logging entries, or Feed Change Notification
payloads are treated as unrecoverable poison input. The relay logs concise
diagnostics and returns HTTP `204` without calling the destination so Pub/Sub
does not retry malformed messages.

Missing runtime configuration, destination auth/config failures, transient
destination failures, and unexpected delivery exceptions return non-2xx so
Pub/Sub can retry according to the subscription policy.

## Storage Boundary

The relay does not read or write AlloyDB. It does not poll `feed_audit_events`,
does not create delivery state, and does not import storage-layer SQL or feed
store modules. `feed_audit_events` remains the canonical durable audit ledger.
