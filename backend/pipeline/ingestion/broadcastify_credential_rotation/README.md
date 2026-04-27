# Broadcastify Credential Rotation

A Google Cloud Function that periodically refreshes Broadcastify authentication
credentials and stores a fresh signed JWT in Secret Manager. It is triggered on
a schedule by Cloud Scheduler.

## How It Works

1. **Unauthenticated JWT** — The function generates a short-lived JWT signed
   with the Broadcastify API key (`BROADCASTIFY_API_KEY`). This token is used
   only to authenticate the login request itself.

2. **Login** — The unauthenticated JWT is sent as a `Bearer` token to the
   Broadcastify auth endpoint (`https://api.bcfy.io/common/v1/auth`) together
   with the account username and password. A successful response returns a
   user ID (`uid`) and a user token (`utk`).

3. **Authenticated JWT** — A second JWT is generated, this time embedding the
   `uid` (as `sub`) and `utk` claims from the login response. This is the
   credential that downstream collectors use to call Broadcastify APIs.

4. **Secret Manager** — The authenticated JWT is written as a new version of
   the Secret Manager secret identified by `BROADCASTIFY_JWT_SECRET_ID`.
   Consumers (e.g. `bcfy_calls_collector`) read `<secret>/versions/latest` at
   runtime.

5. **Old version cleanup** — After writing the new version, secret versions
   older than 6 hours are destroyed automatically.

## ⚠️ Important Token Behaviour

**Password changes invalidate the token.** The issued token is expired as soon
as the account owner changes or resets their password on Broadcastify.com. If
that happens, update the `BROADCASTIFY_PASSWORD` secret and wait for (or
manually trigger) the next rotation run.

**One active token per account.** Issuing a new token for a user immediately
expires any previously issued token for that account. This means that **each
deployment environment (dev, prod, …) must use a separate Broadcastify login**.
If two environments share the same credentials, their rotation jobs will
continuously invalidate each other's tokens, causing authentication failures for
whichever environment ran its rotation last.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `BROADCASTIFY_USERNAME` | ✅ | Broadcastify account username |
| `BROADCASTIFY_PASSWORD` | ✅ | Broadcastify account password |
| `BROADCASTIFY_API_KEY` | ✅ | Broadcastify API signing secret |
| `BROADCASTIFY_API_APP_ID` | ✅ | Broadcastify application ID (`iss` claim) |
| `BROADCASTIFY_API_KEY_ID` | ✅ | Key ID used in the JWT `kid` header |
| `BROADCASTIFY_JWT_SECRET_ID` | ✅ | Secret Manager secret ID where the JWT is stored |
| `GOOGLE_CLOUD_PROJECT` | ✅ | GCP project ID |

All variables are validated at invocation time; a missing variable raises a
`RuntimeError` before any external calls are made.

## Deployment

The function is packaged as a Docker image (see `Dockerfile`) and deployed as a
**Gen 2 Cloud Function** via the `cloud_function` Terraform module. Cloud
Scheduler sends an HTTP POST on the configured cron schedule to trigger a
rotation cycle.

The Secret Manager secret (`BROADCASTIFY_JWT_SECRET_ID`) must exist before the
function is first invoked. It is expected to be created by Terraform.

## Running Tests

```bash
uv run python -m pytest backend/pipeline/ingestion/broadcastify_credential_rotation/tests/ -q
```
