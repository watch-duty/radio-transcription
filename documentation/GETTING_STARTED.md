# Getting Started Guide

Welcome to the **Radio Transcription** project! This guide provides instructions for setting up the radio transcription system and quick-start commands for building, running, testing, and deploying.

---

## System Prerequisites & Toolchain Setup

Before setting up any component, ensure you have installed the required toolchain:

1. **Mise** (Task runner & tool manager): `curl https://mise.run | sh` or `brew install mise`
2. **Docker**: `brew install --cask docker` (Required for local pipeline containers & model evaluation runtimes)
3. **Python `uv`**: `pip install uv` or `brew install uv` (Package manager for backend & model packages)
4. **Node.js & Yarn**: Install Node.js (v18+) and global Yarn (`npm install --global yarn`)
5. **Google Cloud SDK** *(Optional, required for GCP hybrid remote development & deployment)*: `brew install --cask google-cloud-sdk`
6. **Terraform**: `brew install terraform`

### Initialize Toolchain & Pre-commit Hooks

Run the following commands from the root of the repository:

```bash
# Install all required toolchain versions via mise
mise install

# Install local pre-commit hooks for linting/formatting checks
uv run pre-commit install
```

---

## 🎙️ Pillar 1: Radio Transcription Pipeline (Backend)

The backend pipeline ingests audio from multiple sources (Icecast streams, Broadcastify Calls API, Watch Duty Echo, Fire Notifications), segments speech via Apache Beam / Dataflow Voice Activity Detection (VAD), transcribes audio via Speech-to-Text models, evaluates keyword rules, and dispatches notification alerts.

### 1. Generate Protocol Buffers

Python bindings for schema types in `protos/` are generated locally using `mise`:

```bash
mise run generate:protos
```

*(Alternatively: `uv run python -m grpc_tools.protoc -I protos --python_out=backend/pipeline/schema_types --grpc_python_out=backend/pipeline/schema_types --pyi_out=backend/pipeline/schema_types protos/*.proto`)*

### 2. Run 100% Local Pipeline Environment

You can launch the full backend pipeline (Pub/Sub emulator, GCS emulator, ingestion services, rules engine, transcripts service, and PostgreSQL database) locally using Docker:

```bash
# Start the full local backend & database environment
mise run dev
```

#### Transcriber Engine Selection:
* **Default (Mock Transcriber):** By default, local development uses a fast, lightweight mock transcriber.
* **Local Whisper Engine:** To test with real local Speech-to-Text inference, set `TRANSCRIBER_TYPE=local_whisper` in `local_dev/LOCAL.env` and start via:
  ```bash
  mise run dev:whisper
  ```

#### Useful Pipeline Management Commands:
```bash
# View container logs
mise run dev:log

# Stop the local environment and remove volumes
mise run dev:stop
```

### 3. Mock Audio File Ingestion

To simulate incoming emergency radio audio streams locally:

```bash
# Add custom audio file for testing a specific feed
mise run dev:add-audio <data_source> <feed_id> <path/to/audio.flac>

# Examples:
mise run dev:add-audio broadcastify_calls 2912 local_dev/mock_audio/test_bcfy.flac
mise run dev:add-audio fire_notifications RECORDINGS/SAN-JOSE-DISP local_dev/mock_audio/test_bcfy.flac
```

### 4. Running Backend Tests

```bash
# 1. Run database component tests (uses testcontainers)
mise run test:component

# 2. Run API service tests
mise run test:api

# 3. Run End-to-End pipeline tests (isolated environment)
mise run test:e2e

# 4. Run live environment regression test suite (sequential)
mise run test:regression
```

---

## 🤖 Pillar 2: ASR Model & Evaluation Infrastructure

The model infrastructure provides tools for evaluating Speech-to-Text models (Whisper, NeMo/Canary, Gemini, Granite, Cohere), managing canonical manifests, and running Gemini Supervised Fine-Tuning (SFT).

### 1. Local ASR Experiment Runtimes (Docker)

Spin up lightweight ASR evaluation containers from the root directory:

```bash
# Launch Jupyter Notebook server in lightweight CPU runtime (accessible at http://localhost:8888)
docker compose -f asr-eval-docker-compose.yml up -d notebooks-cpu

# Launch lightweight GPU container (requires NVIDIA Docker support)
docker compose -f asr-eval-docker-compose.yml up -d notebooks

# Launch interactive shell for NeMo/Canary CLI workflows
docker compose -f asr-eval-docker-compose.yml run nemo-cli-cpu
```

### 2. Provisioning Dedicated GPU Evaluation VMs on GCP

For heavy GPU evaluations or NeMo training, provision a dedicated Compute Engine instance using Terraform:

```bash
cd terraform/modules/asr_evaluation

# Create local tfvars file
cat <<EOF > local_variables.tfvars
name       = "your-name-asr-eval"
project_id = "your-gcp-project-id"
EOF

terraform init
terraform apply -var-file=local_variables.tfvars
```

#### Connect to GPU VM and Forward Port:
```bash
# Authenticate gcloud Application Default Credentials on VM
gcloud compute ssh your-name-asr-eval --project your-gcp-project-id --zone us-central1-a -- -L 8888:localhost:8888
```

### 3. Running Gemini Supervised Fine-Tuning (`gemini-sft` CLI)

Run Gemini SFT workflows inside the `notebooks-cpu` container:

```bash
# 1. Prepare an SFT round (validates manifest schema, duplicate URIs, and GCS state)
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft prepare --config /path/to/run_config.toml'

# 2. Submit or resume a paid Vertex AI tuning job
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft tune --config /path/to/run_config.toml --confirm'

# 3. Run evaluation & generate WER summary reports
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft eval --config /path/to/run_config.toml'
```

---

## 🔌 Pillar 3: Management & Proxy APIs

The management API stack consists of the Transcripts API, Rules Management API, Feed Management API, and the Node/Express Frontend Proxy API (`frontend/api`).

### 1. OpenAPI Specification Generation

The API controllers use `tsoa` to generate OpenAPI specs (`openapi.yaml`) and inject Google OAuth metadata:

```bash
# Compile TypeScript controllers and post-process openapi.yaml with x-google-auth
yarn --cwd frontend/api generate-spec
```

### 2. Google Cloud Identity RBAC Setup

To manage administrator permissions, the backend queries Google Workspace / Cloud Identity groups using Cloud Identity APIs:

1. Create a Google Group (e.g., `radio-transcription-admins@YOUR_DOMAIN.org`).
2. Add your GCP backend Service Account email as a member of the group.
3. Configure environment variable:
   ```ini
   WORKSPACE_ADMIN_GROUP_EMAIL=radio-transcription-admins@YOUR_DOMAIN.org
   ```
   *(Note: In local development, if `WORKSPACE_ADMIN_GROUP_EMAIL` is omitted, all authenticated users are granted admin access automatically).*

---

## 🖥️ Pillar 4: Frontend UI Tool

The frontend application (`frontend/transcription-ui`) is built with React, TypeScript, and Vite, providing live transcript viewing, stream monitoring, and rule administration.

### 1. Option A: Run UI with 100% Local Pipeline

When running `mise run dev`, both the React UI (port `:5173`) and the API Gateway (port `:8080`) are booted automatically. Open `http://localhost:5173` in your browser.

### 2. Option B: Hybrid Remote UI Development against GCP Dev Backend

To develop local frontend UI code while connecting directly to Cloud Run services in GCP:

1. **One-Time GCP Authentication:**
   ```bash
   gcloud init
   gcloud config set account <your-email@domain.com>
   gcloud config set project <your-dev-project-id>

   # Authenticate ADC with Service Account impersonation
   gcloud auth application-default login --impersonate-service-account=<API_SERVICE_ACCOUNT>@<PROJECT_ID>.iam.gserviceaccount.com
   ```

2. **Initialize Remote Environment Configs:**
   ```bash
   mise run dev:remote:init
   ```
   This task fetches dev Cloud Run endpoints and writes `frontend/api/.env.local` and `frontend/transcription-ui/.env.dev.local`.

3. **Launch Hybrid Frontend:**
   ```bash
   # Run both local FE Proxy API (:8080) and UI (:5173) against GCP backend
   mise run dev:remote

   # Alternatively, run UI only (proxying directly to remote API Gateway)
   mise run dev:remote:ui
   ```

---

## ☁️ Pillar 5: Deployment Infrastructure

Deployment infrastructure for the Radio Transcription service is defined in Terraform and managed via isolated GitHub Actions workflows.

### 1. Local Infrastructure Verification

Before pushing Terraform changes, verify formatting and execution plan:

```bash
# Format and lint Terraform HCL across the repo
mise run check

# Run local terraform plan (requires GCP credentials)
cd terraform/environments/prod
terraform init
terraform plan
```

### 2. Deployment CI/CD Workflows

* **Dev Environment Deployment:** Merging a pull request into `main` automatically triggers the **Deploy (dev)** workflow.
* **Production Environment Deployment:** Production releases are manually triggered via the **Deploy (prod)** workflow.
* **Production Deploy Lock:** To prevent state corruption or race conditions, production releases lease an atomic GCS lock (`gs://radio_transcription_prod_deploy_lock/prod_deploy.lock`).

### 3. Monitoring Dashboards & Runbooks

* **Cloud Monitoring Dashboard UI-to-Code Sync:**
  ```bash
  # Export edited System Health dashboard from GCP UI to parameterized Terraform JSON
  mise run dashboard:export
  ```
* **Infrastructure Runbooks:**
  * Refer to [Dataflow IAM Runbook](file:///usr/local/google/home/stephaniechew/watch_duty/radio-transcription-deployment/docs/dataflow-iam-runbook.md) for IAM permissions.
  * Refer to [Dataflow Tuning Runbook](file:///usr/local/google/home/stephaniechew/watch_duty/radio-transcription-deployment/docs/dataflow-tuning.md) for streaming pipeline performance optimization.

---

## 💡 Code Formatting & Pre-Commit Validation

Before committing code modifications to any pillar, run the formatters and linters:

```bash
# Format Python, TypeScript, and Markdown files
mise run format

# Lint all code and Jupyter notebooks
mise run lint
```
## System Prerequisites & Toolchain Setup

Before setting up any component, ensure you have installed the required toolchain:

1. **Mise** (Task runner & tool manager): `curl https://mise.run | sh` or `brew install mise`
2. **Docker**: `brew install --cask docker` (Required for local pipeline containers & model evaluation runtimes)
3. **Python `uv`**: `pip install uv` or `brew install uv` (Package manager for backend & model packages)
4. **Node.js & Yarn**: Install Node.js (v18+) and global Yarn (`npm install --global yarn`)
5. **Google Cloud SDK** *(Optional, required for GCP hybrid remote development & deployment)*: `brew install --cask google-cloud-sdk`
6. **Terraform**: `brew install terraform`

### Initialize Toolchain & Pre-commit Hooks

Run the following commands from the root of the repository:

```bash
# Install all required toolchain versions via mise
mise install

# Install local pre-commit hooks for linting/formatting checks
uv run pre-commit install
```

---

## 🎙️ Pillar 1: Radio Transcription Pipeline (Backend)

The backend pipeline ingests audio from multiple sources (Icecast streams, Broadcastify Calls API, Watch Duty Echo, Fire Notifications), segments speech via Apache Beam / Dataflow Voice Activity Detection (VAD), transcribes audio via Speech-to-Text models, evaluates keyword rules, and dispatches notification alerts.

### 1. Generate Protocol Buffers

Python bindings for schema types in `protos/` are generated locally using `mise`:

```bash
mise run generate:protos
```

*(Alternatively: `uv run python -m grpc_tools.protoc -I protos --python_out=backend/pipeline/schema_types --grpc_python_out=backend/pipeline/schema_types --pyi_out=backend/pipeline/schema_types protos/*.proto`)*

### 2. Run 100% Local Pipeline Environment

You can launch the full backend pipeline (Pub/Sub emulator, GCS emulator, ingestion services, rules engine, transcripts service, and PostgreSQL database) locally using Docker:

```bash
# Start the full local backend & database environment
mise run dev
```

#### Transcriber Engine Selection:
* **Default (Mock Transcriber):** By default, local development uses a fast, lightweight mock transcriber.
* **Local Whisper Engine:** To test with real local Speech-to-Text inference, set `TRANSCRIBER_TYPE=local_whisper` in `local_dev/LOCAL.env` and start via:
  ```bash
  mise run dev:whisper
  ```

#### Useful Pipeline Management Commands:
```bash
# View container logs
mise run dev:log

# Stop the local environment and remove volumes
mise run dev:stop
```

### 3. Mock Audio File Ingestion

To simulate incoming emergency radio audio streams locally:

```bash
# Add custom audio file for testing a specific feed
mise run dev:add-audio <data_source> <feed_id> <path/to/audio.flac>

# Examples:
mise run dev:add-audio broadcastify_calls 2912 local_dev/mock_audio/test_bcfy.flac
mise run dev:add-audio fire_notifications RECORDINGS/SAN-JOSE-DISP local_dev/mock_audio/test_bcfy.flac
```

### 4. Running Backend Tests

```bash
# 1. Run database component tests (uses testcontainers)
mise run test:component

# 2. Run API service tests
mise run test:api

# 3. Run End-to-End pipeline tests (isolated environment)
mise run test:e2e

# 4. Run live environment regression test suite (sequential)
mise run test:regression
```

---

## 🤖 Pillar 2: ASR Model & Evaluation Infrastructure

The model infrastructure provides tools for evaluating Speech-to-Text models (Whisper, NeMo/Canary, Gemini, Granite, Cohere), managing canonical manifests, and running Gemini Supervised Fine-Tuning (SFT).

### 1. Local ASR Experiment Runtimes (Docker)

Spin up lightweight ASR evaluation containers from the root directory:

```bash
# Launch Jupyter Notebook server in lightweight CPU runtime (accessible at http://localhost:8888)
docker compose -f asr-eval-docker-compose.yml up -d notebooks-cpu

# Launch lightweight GPU container (requires NVIDIA Docker support)
docker compose -f asr-eval-docker-compose.yml up -d notebooks

# Launch interactive shell for NeMo/Canary CLI workflows
docker compose -f asr-eval-docker-compose.yml run nemo-cli-cpu
```

### 2. Provisioning Dedicated GPU Evaluation VMs on GCP

For heavy GPU evaluations or NeMo training, provision a dedicated Compute Engine instance using Terraform:

```bash
cd terraform/modules/asr_evaluation

# Create local tfvars file
cat <<EOF > local_variables.tfvars
name       = "your-name-asr-eval"
project_id = "your-gcp-project-id"
EOF

terraform init
terraform apply -var-file=local_variables.tfvars
```

#### Connect to GPU VM and Forward Port:
```bash
# Authenticate gcloud Application Default Credentials on VM
gcloud compute ssh your-name-asr-eval --project your-gcp-project-id --zone us-central1-a -- -L 8888:localhost:8888
```

### 3. Running Gemini Supervised Fine-Tuning (`gemini-sft` CLI)

Run Gemini SFT workflows inside the `notebooks-cpu` container:

```bash
# 1. Prepare an SFT round (validates manifest schema, duplicate URIs, and GCS state)
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft prepare --config /path/to/run_config.toml'

# 2. Submit or resume a paid Vertex AI tuning job
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft tune --config /path/to/run_config.toml --confirm'

# 3. Run evaluation & generate WER summary reports
docker compose -f asr-eval-docker-compose.yml run --rm notebooks-cpu \
  bash -lc 'gemini-sft eval --config /path/to/run_config.toml'
```

---

## 🔌 Pillar 3: Management & Proxy APIs

The management API stack consists of the Transcripts API, Rules Management API, Feed Management API, and the Node/Express Frontend Proxy API (`frontend/api`).

### 1. OpenAPI Specification Generation

The API controllers use `tsoa` to generate OpenAPI specs (`openapi.yaml`) and inject Google OAuth metadata:

```bash
# Compile TypeScript controllers and post-process openapi.yaml with x-google-auth
yarn --cwd frontend/api generate-spec
```

### 2. Google Cloud Identity RBAC Setup

To manage administrator permissions, the backend queries Google Workspace / Cloud Identity groups using Cloud Identity APIs:

1. Create a Google Group (e.g., `radio-transcription-admins@YOUR_DOMAIN.org`).
2. Add your GCP backend Service Account email as a member of the group.
3. Configure environment variable:
   ```ini
   WORKSPACE_ADMIN_GROUP_EMAIL=radio-transcription-admins@YOUR_DOMAIN.org
   ```
   *(Note: In local development, if `WORKSPACE_ADMIN_GROUP_EMAIL` is omitted, all authenticated users are granted admin access automatically).*

---

## 🖥️ Pillar 4: Frontend UI Tool

The frontend application (`frontend/transcription-ui`) is built with React, TypeScript, and Vite, providing live transcript viewing, stream monitoring, and rule administration.

### 1. Option A: Run UI with 100% Local Pipeline

When running `mise run dev`, both the React UI (port `:5173`) and the API Gateway (port `:8080`) are booted automatically. Open `http://localhost:5173` in your browser.

### 2. Option B: Hybrid Remote UI Development against GCP Dev Backend

To develop local frontend UI code while connecting directly to Cloud Run services in GCP:

1. **One-Time GCP Authentication:**
   ```bash
   gcloud init
   gcloud config set account <your-email@domain.com>
   gcloud config set project <your-dev-project-id>

   # Authenticate ADC with Service Account impersonation
   gcloud auth application-default login --impersonate-service-account=<API_SERVICE_ACCOUNT>@<PROJECT_ID>.iam.gserviceaccount.com
   ```

2. **Initialize Remote Environment Configs:**
   ```bash
   mise run dev:remote:init
   ```
   This task fetches dev Cloud Run endpoints and writes `frontend/api/.env.local` and `frontend/transcription-ui/.env.dev.local`.

3. **Launch Hybrid Frontend:**
   ```bash
   # Run both local FE Proxy API (:8080) and UI (:5173) against GCP backend
   mise run dev:remote

   # Alternatively, run UI only (proxying directly to remote API Gateway)
   mise run dev:remote:ui
   ```

---

## ☁️ Pillar 5: Deployment Infrastructure

Deployment infrastructure for the Radio Transcription service is defined in Terraform and managed via isolated GitHub Actions workflows.

### 1. Local Infrastructure Verification

Before pushing Terraform changes, verify formatting and execution plan:

```bash
# Format and lint Terraform HCL across the repo
mise run check

# Run local terraform plan (requires GCP credentials)
cd terraform/environments/prod
terraform init
terraform plan
```

### 2. Deployment CI/CD Workflows

* **Dev Environment Deployment:** Merging a pull request into `main` automatically triggers the **Deploy (dev)** workflow.
* **Production Environment Deployment:** Production releases are manually triggered via the **Deploy (prod)** workflow.
* **Production Deploy Lock:** To prevent state corruption or race conditions, production releases lease an atomic GCS lock (`gs://radio_transcription_prod_deploy_lock/prod_deploy.lock`).

### 3. Monitoring Dashboards & Runbooks

* **Cloud Monitoring Dashboard UI-to-Code Sync:**
  ```bash
  # Export edited System Health dashboard from GCP UI to parameterized Terraform JSON
  mise run dashboard:export
  ```
* **Infrastructure Runbooks:**
  * Refer to [Dataflow IAM Runbook](file:///usr/local/google/home/stephaniechew/watch_duty/radio-transcription-deployment/docs/dataflow-iam-runbook.md) for IAM permissions.
  * Refer to [Dataflow Tuning Runbook](file:///usr/local/google/home/stephaniechew/watch_duty/radio-transcription-deployment/docs/dataflow-tuning.md) for streaming pipeline performance optimization.

---

## 💡 Code Formatting & Pre-Commit Validation

Before committing code modifications to any pillar, run the formatters and linters:

```bash
# Format Python, TypeScript, and Markdown files
mise run format

# Lint all code and Jupyter notebooks
mise run lint
```
