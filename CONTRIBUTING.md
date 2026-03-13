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
AUDIO_STAGING_BUCKET=wd-radio-test
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