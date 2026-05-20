# Contributing

## Pre-requisites

1. Install Mise (`curl https://mise.run | sh` or `brew install mise` - https://mise.jdx.dev/getting-started.html)
2. Install tools: `mise install`
3. Optionally activate mise venv: `eval "$(mise activate zsh)"` (see docs above for other options)
4. Install Docker: `brew install --cask docker`

## Backend tools

* Language: Python
* Package management: `uv`
* Formatting and linting: `ruff`
* Type-checking: `ty check`
* Unit testing: Python `unittest`

## Pipeline E2E Local Development
On a high level, this local pipeline runs the following:
#### Shared infrastructure
1. Pub/Sub emulator (manages all PubSub topics for each Pub/Sub instance in the pipeline)
2. GCS emulator (manages all GCS buckets for audio storage in the pipeline)
3. Mock Icecast server (simulates audio streams for testing)

#### Pipeline
1. Audio ingestion service (fetches audio from streams and uploads to GCS)
2. Transcription pipeline service (for processing audio into transcript text)
3. Rules Evaluation service (to process transcription events)
4. Notification service (to send alerts when rules match)

#### API Management Services
1. Rules Management service (to manage keywords and evaluation logic)
2. Transcripts API services
3. Frontend API (proxy for rules, transcript, and feed management)
4. Mock server (to receive and display mock notifications)

Integration tests run an automated E2E test on startup.

> [!NOTE]
> `local_dev/test_data.sql` is used to seed the database with dummy feeds and rules for local development (`mise dev:start`). It is explicitly ignored in the integration tests (`mise test:e2e`) to ensure tests run in a clean, isolated database environment.
> Also, we are currently missing audio ingestion for API polling and Echoes.

Locally run the full pipeline from E2E:
```bash
mise dev:start
mise dev:log # to see logs for one or all containers
mise dev:stop # to stop containers & rm volumes
```

Send a test payload to the Transcription PubSub (ingested by the Rules Evaluation service) to test the path from the Rules Evaluation service to the Notification service.
```bash
# Note: This script is run automatically by the integration-test service on startup.
# To run it again manually, use the following command:
docker-compose exec rules-evaluation python /app/test_evaluation_publish.py
```

## Integration and E2E Tests
We categorize our non-unit tests into three levels to balance speed and coverage. These are located under `integration_tests/`:

1. **Component Tests**: Isolated tests for database stores using `testcontainers`.
   * Run all: `mise run test:component`
   * Run specific: `mise run test:component:rules` or `test:component:feeds`

2. **API Tests**: Tests targeting running services via HTTP.
   * Run all: `mise run test:api`

3. **End-to-End (E2E) Tests**: Full system flow tests involving multiple services and the Pub/Sub emulator.
   * Run in an isolated environment (Docker handles lifecycle): `mise run test:e2e`
   * Run against a running background environment: `mise run test:e2e:local` (Requires you to start the environment first)

For more details on the architecture and local execution of the pipeline, see the **Pipeline E2E Local Development** section above.

## Frontend tools

* Language: Typescript
* Package management: `yarn` (install with `npm install --global yarn`)
* Formatting and linting: `prettier` and `eslint`
* Bundling: `vite` (https://vite.dev/)
* Testing: [Vitest](https://vitest.dev/) with [React Testing Library](https://testing-library.com/react)
* Install Node (https://nodejs.org/en/download/)
* (Optional) Install Firebase CLI (https://firebase.google.com/docs/cli) for hosting deployments


## Frontend Development

When you finish this section, you'll have the proxy API running on `:8080` and the UI on `:5173`, with Google sign-in working and the UI reading from the dev backend services in GCP.

The frontend consists of two pieces that must both be running in separate terminals:
- **Proxy API** (`frontend/api`) — Node/Express service on `:8080` that handles auth and forwards requests to the dev backend Cloud Run services.
- **UI** (`frontend/transcription-ui`) — Vite-served React app on `:5173`.

All commands below assume you're running from the top level of the repo.

### 1. One-time setup

#### 1a. Install gcloud and sign in

Install the gcloud CLI (https://docs.cloud.google.com/sdk/docs/install-sdk), then:

```bash
gcloud init                                  # follow prompts; pick the GCP project
gcloud config set account <your work email>
gcloud config set project <your project ID>
```

#### 1b. Gather project-specific values

You'll plug these into `.env.local` files later. Look them up now:

- **GCP project ID** — GCP Console project picker, or `gcloud projects list`.
- **Service account name** for the proxy API — GCP Console → IAM & Admin → Service Accounts (look for one named like `*-api-dev`), or:
  ```bash
  gcloud iam service-accounts list --project=<your project ID>
  ```
- **OAuth 2.0 Web application Client ID and Secret** — GCP Console → APIs & Services → Credentials → "OAuth 2.0 Client IDs". Pick the entry of type "Web application". Verify that `http://localhost:5173` is listed under **Authorized JavaScript origins** (add and save if it isn't — changes take ~30s to propagate).
- **OAuth Client Secret** — same Credentials page. Newer clients only show it once at creation. If hidden, check Secret Manager:
  ```bash
  gcloud secrets list --project=<your project ID>           # look for one matching oauth/client/auth
  gcloud secrets versions access latest --secret=<secret-name> --project=<your project ID>
  ```
  > **Note on copy-pasting secrets:** ensure no trailing whitespace ends up in your `.env.local`. A trailing `%` in your terminal output from `gcloud secrets versions access` is a zsh display marker indicating no trailing newline — it is **not** part of the secret value.

  If neither source has a usable value, **Reset Secret** on the OAuth client in the Console — coordinate with the team first, since this invalidates the secret in any deployed environment using it.

#### 1c. Get permission to impersonate the service account

The proxy API makes calls to GCP services as the service account, so your user account needs the "Service Account Token Creator" role on that SA.

> **Note:** this binding command requires `roles/iam.serviceAccountAdmin` on the service account, which most developers don't have. If you hit `PERMISSION_DENIED`, ask an admin to run it for you.

```bash
export SA_NAME=<your service account name for the API>
export PROJECT_ID=$(gcloud config get-value project)
export USER_EMAIL=$(gcloud config get-value account)
gcloud iam service-accounts add-iam-policy-binding \
    $SA_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    --member="user:$USER_EMAIL" \
    --role="roles/iam.serviceAccountTokenCreator"
```

#### 1d. Impersonate the service account for Application Default Credentials

```bash
export SA_NAME=<your service account name for the API>
export PROJECT_ID=$(gcloud config get-value project)
gcloud auth application-default login --impersonate-service-account=$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com
```

When you're done with frontend work, switch back to your default account with:
```bash
gcloud auth application-default login
```

#### 1e. Install shared frontend dependencies

The `frontend/common` package is consumed by both the API and the UI — build it once before installing either:

```bash
yarn --cwd frontend/common install && yarn --cwd frontend/common build
```

### 2. Configure and run the proxy API

1. Create `frontend/api/.env.local`:

```bash
cat <<EOF > frontend/api/.env.local
ALLOWED_ORIGIN=http://localhost:5173
TRANSCRIPTS_API_URL=<your URL for transcripts API>/v1/transcripts
RULES_API_URL=<your URL for rules API>/v1/rules
FEEDS_STORE_API_URL=<your URL for feeds store API>/v1/feeds
PROJECT_ID=<your project ID>
API_PUBLIC_URL=http://localhost:5173
GOOGLE_AUTH_CLIENT_ID=<your Google Auth client ID>
GOOGLE_AUTH_CLIENT_SECRET=<your Google Auth client secret>
EOF
```

The three `*_API_URL` values point to Cloud Run services in the GCP project. List them with:
```bash
gcloud run services list --project=<your project ID>
```
Look for services named like `transcripts-api-dev`, `rules-management-dev`, and `feed-store-dev`. **The `/v1/<resource>` path suffix is required** — the proxy appends resource IDs directly to these URLs and will 404 without it.

2. Install package dependencies:
```bash
yarn --cwd frontend/api install
```

3. Run the API (leave this terminal open):
```bash
yarn --cwd frontend/api local
```

The API listens on `http://localhost:8080`.

### 3. Configure and run the UI

In a **second terminal**:

1. Create `frontend/transcription-ui/.env.local-dev`. (Vite reserves `.env.local` for itself, so we use `.env.local-dev` instead.)

```bash
cat <<EOF > frontend/transcription-ui/.env.local-dev
VITE_GOOGLE_AUTH_CLIENT_ID=<your Google OAuth 2.0 Client ID — same as GOOGLE_AUTH_CLIENT_ID in frontend/api/.env.local>
VITE_API_BASE_URL=
VITE_ALERT_ICON_SYMBOL_NAME=local_fire_department
EOF
```

**Leave `VITE_API_BASE_URL` empty** to route API calls through your local proxy. Do not copy the placeholder from `.env.example` — it's a non-resolvable hostname that will cause cross-origin errors.

`VITE_GOOGLE_AUTH_CLIENT_ID` must be the **same** OAuth client ID you used in the API's `.env.local`.

2. Install package dependencies:
```bash
yarn --cwd frontend/transcription-ui install
```

3. Run the UI:
```bash
yarn --cwd frontend/transcription-ui local
```

4. Open http://localhost:5173/ in your browser and sign in with Google.

> Env file changes are only picked up at startup — restart `yarn ... local` after editing `.env.local` or `.env.local-dev`.


## Making Changes to Files
* run `mise format`
* run `mise lint`


## Pre-commit Hooks
This repository uses `pre-commit` to ensure code quality before pushing.
To install the pre-commit hook in your local Git repository:
```bash
uv run pre-commit install
```
After installation, `pre-commit` will automatically run on the changed files during `git commit`.

You can also run the pre-commit hooks manually on all files at any time:
```bash
uv run pre-commit run --all-files
```

## Deployments and Local Testing
* Docker

## Debugging

### Github Workflows
If there is a workflow that is failing, and for the life of you, you cannot figure out why, you can open an SSH session into the workflow. Rerun the job by triggering a manual workflow.
![Manual workflow trigger instructions](manual_workflow_trigger.png)
Note that this is only available on workflows that have it configured. If you want to configure it for a new workflow, you'll need open a new PR and merge the configuration into main before the option is available for you.
