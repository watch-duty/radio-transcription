# Contributing

## Pre-requisites

1. Install Mise (`curl https://mise.run | sh` or `brew install mise` - https://mise.jdx.dev/getting-started.html)
2. Install tools: `mise install`
3. Optionally activate mise venv: `eval "$(mise activate zsh)"` (see docs above for other options)
4. Install Docker: `brew install --cask docker`

## Dev/coding tools and best practices

### Backend tools

* Language: Python
* Package management: `uv`
* Formatting and linting: `ruff`
* Type-checking: `ty check`
* Unit testing: Python `unittest`

### E2E Local Development
On a high level, this local pipeline runs the following:
1. Pub/Sub emulator
2. Rules Management service (to manage keywords and evaluation logic)
3. Rules Evaluation service (to process transcription events)
4. Notification service (to send alerts when rules match)
5. Mock server (to receive and display mock notifications)
6. Integration tests (runs an automated E2E test on startup)

Note that currently the following are missing from the E2E setup:
* Audio ingestion pipeline and storage
* Transcription pipeline and storage
* Rules storage

Locally run the full pipeline from E2E
```bash
docker-compose down -v && docker-compose up --build -d &&
docker-compose logs -f rules-evaluation notification mock-server integration-test
```

Send a test payload to the Transcription PubSub (ingested by the Rules Evaluation service) to test the path from the Rules Evaluation service to the Notification service.
```bash
# Note: This script is run automatically by the integration-test service on startup.
# To run it again manually, use the following command:
docker-compose exec rules-evaluation python /app/test_evaluation_publish.py
```

#### Audio Ingestion
##### Icecast Collector
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
export ICECAST_STREAM_URL=https://example.com
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

### Frontend tools

* Language: Typescript
* Package management: `yarn`
* Formatting and linting: `prettier` and `eslint`
* Bundling: `vite` (https://vite.dev/)
* Testing: [Vitest](https://vitest.dev/) with [React Testing Library](https://testing-library.com/react)


### Making Changes to Files
* run `mise format`
* run `mise lint`

### Deployments and Local Testing
* Docker