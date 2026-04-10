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

## E2E Local Development
On a high level, this local pipeline runs the following:
1. Pub/Sub emulator (manages all PubSub topics for each Pub/Sub instance in the pipeline)
2. Rules Management service (to manage keywords and evaluation logic)
3. Rules Evaluation service (to process transcription events)
4. Notification service (to send alerts when rules match)
5. Mock server (to receive and display mock notifications)
6. Frontend API (for rules, transcript, and feed management)

Integration tests run an automated E2E test on startup.

Note that currently the following are missing from the E2E setup:
* Audio ingestion pipeline and storage
* Transcription pipeline and storage
* Rules storage

Locally run the full pipeline from E2E
```bash
VITE_GOOGLE_AUTH_CLIENT_ID="<INSERT AUTH CLIENT ID>" &&
docker-compose down -v && docker-compose up --build -d &&
docker-compose logs -f \
  transcripts-api\
  rules-evaluation\
  rules-management\
  notification\
  mock-server\
  frontend-api
```

Send a test payload to the Transcription PubSub (ingested by the Rules Evaluation service) to test the path from the Rules Evaluation service to the Notification service.
```bash
# Note: This script is run automatically by the integration-test service on startup.
# To run it again manually, use the following command:
docker-compose exec rules-evaluation python /app/test_evaluation_publish.py
```

### Audio Ingestion
#### Icecast Collector
*Installation*
1. Install ffmpeg
```
brew install ffmpeg
```

2. Install the gcloud cli tool
https://docs.cloud.google.com/sdk/docs/install-sdk
```
gcloud init
gcloud auth login
```

*Building & Running Locally*
```
# Assuming you're running from the top level of the root dir
source .venv/bin/activate
export BROADCASTIFY_USERNAME=<your broadcastify username>
export BROADCASTIFY_PASSWORD=<your broadcastify pword>
export ICECAST_SOURCE_FEED_ID=123
python backend/pipeline/ingestion/collectors/local_icecast_collector.py

<optional env variable>
export ICECAST_LOCAL_OUTPUT_DIR="/tmp/audio_chunks"
```

*Building & Running with Docker*
```
# Assuming you're running from the top level of the root dir.
# Run this command if you are running this for the first time.
cat <<EOF > backend/pipeline/ingestion/collectors/.icecast_env
BROADCASTIFY_USERNAME=<your broadcastify username>
BROADCASTIFY_PASSWORD=<your broadcastify pword>
AUDIO_STAGING_BUCKET=<your audio staging bucket>
PUBSUB_TOPIC_PATH=<your pubsub topic path>
ALLOYDB_HOST=<your alloydb host>
ALLOYDB_USER=<your alloydb user>
ALLOYDB_DB=<your alloydb database name>
# Optional: uncomment and set if needed by your deployment
# ALLOYDB_PORT=<your alloydb port, e.g. 5432>
# ALLOYDB_PASSWORD=<your alloydb password>
EOF

docker build -t "icecast" -f backend/pipeline/ingestion/collectors/Dockerfile .
docker run -v ~/.config/gcloud:/.config/gcloud \
           --env-file backend/pipeline/ingestion/collectors/.icecast_env \
           -e GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json \
           -it icecast
```

## Integration Tests
There is a basic set of integration tests that are currently run against the local pipeline.
These can be found under /integration_tests. Make sure to build and run the pipeline locally
before running.
```
docker compose run --rm integration-tests
```

## Frontend tools

* Language: Typescript
* Package management: `yarn` (install with `npm install --global yarn`)
* Formatting and linting: `prettier` and `eslint`
* Bundling: `vite` (https://vite.dev/)
* Testing: [Vitest](https://vitest.dev/) with [React Testing Library](https://testing-library.com/react)
* Install Node (https://nodejs.org/en/download/)
* (Optional) Install Firebase CLI (https://firebase.google.com/docs/cli) for hosting deployments


## Frontend Development

### Proxy API Development

_Installation_

1. Install the gcloud cli tool https://docs.cloud.google.com/sdk/docs/install-sdk
```
gcloud init
```

_Prerequisites_

This is assuming that you want the local proxy API to make calls to the Google Cloud services in your GCP project (as opposed to the backend services running locally in Docker).

1. Grant yourself the "Service Account Token Creator" role on the service account associated with the proxy API
```bash
# Run this command if you are running this for the first time.
export SA_NAME=<your service account name for the API>
export PROJECT_ID=$(gcloud config get-value project)
export USER_EMAIL=$(gcloud config get-value account)
gcloud iam service-accounts add-iam-policy-binding \
    $SA_NAME@$PROJECT_ID.iam.gserviceaccount.com \
    --member="user:$USER_EMAIL" \
    --role="roles/iam.serviceAccountTokenCreator"
```

2. Impersonate the service account
```bash
# Run this command if you have not impersonated the service account or authenticated with your default account
export SA_NAME=<your service account name for the API>
export PROJECT_ID=$(gcloud config get-value project)
gcloud auth application-default login --impersonate-service-account=$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com
```

Note that when you are done you can switch back to your default account by running:
```bash
gcloud auth application-default login
```

_Building & Running Locally_

1. The proxy API uses `dotenv` to configure the environment, which looks for the file `.env.local` in the `frontend/api` directory. Either copy `.env.example` to `.env.local`, or create it from scratch using the below command:

```bash
# Assuming you're running from the top level of the root dir.
# Run this command if you are running this for the first time.
cat <<EOF > frontend/api/.env.local
ALLOWED_ORIGIN=http://localhost:5173
TRANSCRIPTS_API_URL=<your URL for transcripts API>
RULES_API_URL=<your URL for rules API>
EOF
```

2. Install the package dependencies
```bash
# Assuming you're running from the top level of the root dir.
yarn --cwd frontend/api install
```

3. Run the API locally
```bash
# Assuming you're running from the top level of the root dir.
yarn --cwd frontend/api local
```

### UI Development

_Building & Running Locally_

1. The frontend UI uses `dotenv` to configure the environment, which looks for the file `.env.local` in the `frontend/transcription-ui` directory. Either copy `.env.example` to `.env.local`, or create it from scratch using the below command:

```bash
# Assuming you're running from the top level of the root dir.
# Run this command if you are running this for the first time.
cat <<EOF > frontend/transcription-ui/.env.local
VITE_GOOGLE_AUTH_CLIENT_ID=<your Google OAuth 2.0 Client ID for your project, found under Google Auth Platform>
VITE_API_BASE_URL=<your URL for the API, leave empty to use the local proxy>
EOF
```

2. Install the package dependencies
```bash
# Assuming you're running from the top level of the root dir.
yarn --cwd frontend/transcription-ui install
```

3. Run the UI locally
```bash
# Assuming you're running from the top level of the root dir.
yarn --cwd frontend/transcription-ui local
```

4. Open up a web browser and navigate to http://localhost:5173/


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