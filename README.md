# Radio Transcription

A scalable, real-time open-source system designed for transcribing and managing emergency radio traffic.

---

## 🏛️ Repository Architecture (5 Pillars)

The `radio-transcription` repository and ecosystem contains 5 main offerings:

1. **[Radio Transcription Pipeline](#radio-transcription-pipeline-backendpipeline)**: Real-time audio ingestion collectors, Apache Beam/Dataflow Voice Activity Detection (VAD) audio segmentation for continuous audio, transcription service, rules evaluation, and notification dispatch.
2. **[ASR Model & Evaluation Infrastructure](#asr-model--evaluation-infrastructure-model)**: ASR evaluation notebooks, NeMo/Whisper/Canary model benchmarks, GPU VM provisioning, canonical data manifest schemas, and the `gemini-sft` CLI for Supervised Fine-Tuning.
3. **[Management & Proxy APIs](#management--proxy-apis-frontendapi--backend-services)**: Transcripts API, Rules Management API, Feed Management API, Node/Express FE Proxy Gateway, `tsoa` OpenAPI spec generator, and Google Cloud Identity RBAC.
4. **[Frontend UI Tool](#4-frontend-ui-tool-frontendtranscription-ui)**: A React + TypeScript + Vite web interface for operators to monitor live audio streams, manage feed lifecycle states, configure alert rules, and view transcribed audio.
5. **[Deployment Infrastructure](#5-deployment-infrastructure-radio-transcription-deployment--terraform)**: Infrastructure as Code (IaC) powered by Terraform, automated dev/prod CI/CD GitHub Workflows, production deployment lock mechanism, Dataflow tuning, and Cloud Monitoring dashboards.

---

## 📂 Directory Structure

```
radio-transcription/
├── backend/
│   ├── pipeline/             # Audio ingestion collectors, VAD segmentation, transcription, & rules
│   └── scripts/              # Pipeline helper & load testing scripts
├── documentation/            # Architectural guides, testing docs, & getting started instructions
│   └── GETTING_STARTED.md    # Comprehensive Quick Start Guide for all 5 pillars
├── frontend/
│   ├── api/                  # Express proxy API gateway & Cloud Identity auth
│   ├── common/               # Shared TypeScript schemas and utilities
│   └── transcription-ui/     # React + TypeScript + Vite web UI
├── model/
│   ├── colabs/               # Model evaluation Jupyter notebooks
│   ├── data/                 # Training/eval manifests & Label Studio exports
│   ├── data_sources/         # Audio source fetch scripts (Broadcastify, Echo, Fire Notifications)
│   ├── scripts/sft/          # Gemini Supervised Fine-Tuning (SFT) CLI & operator docs
│   └── src/                  # Shared Python model helpers (`common`, `gemini_sft`)
├── protos/                   # Protocol Buffer schema definitions
├── terraform/                # Infrastructure modules for ASR evaluation VMs
├── ASR_CONTRIBUTING.md       # Detailed guide for model evaluation & Jupyter workflows
├── CONTEXT.md                # Comprehensive domain glossary & terminology
└── CONTRIBUTING.md           # Developer quick start, mise commands, & style guidelines
```

---

## 🚀 Quick Links

* 📘 **[Getting Started Guide](./documentation/GETTING_STARTED.md)** — Step-by-step setup instructions and quick-start commands for all 5 pillars.
* 🛠️ **[Contributing Guide](./CONTRIBUTING.md)** — Pre-requisites, development workflows, pre-commit setup, and coding standards.
* 📖 **[Domain Glossary & Context](./CONTEXT.md)** — Comprehensive definitions of ingestion terms, SFT workflows, and failure handling policies.
* 🤖 **[AI Agent Instructions](./AGENTS.md)** — Guidelines for automated coding agents.

---

## 📚 Repository Documentation Index

### Radio Transcription Pipeline (`backend/pipeline/`)
* [Backend Pipeline Architecture](./backend/pipeline/README.md) — High-level pipeline components and protobuf generation.
* [Collector Authoring & Failure Policy Guide](./backend/pipeline/ingestion/collectors/README.md) — Detailed collector contract, stream types, observation boundaries, and status reason classification.
* [Continuous Audio Segmentation Architecture](./backend/pipeline/segmentation/ARCHITECTURE.md) — Stateful Apache Beam topology, Windmill self-chaining, and dual-timer mechanics.
* [Audio Segmentation Module Overview](./backend/pipeline/segmentation/README.md) — Voice activity detection and stitching transform details.
* [VAD Benchmarks](./backend/pipeline/segmentation/tests/VAD_BENCHMARKS.md) — Benchmark datasets and accuracy metrics for VAD segmentation models.
* [Broadcastify Credential Rotation](./backend/pipeline/ingestion/broadcastify_credential_rotation/README.md) — Operational guide for updating Broadcastify credentials.
* [Local Dev Mock Audio Server](./documentation/local-dev-mock-audio.md) — Guide to mocking continuous audio streams for local testing.
* [Feed Change Webhook Relay](./documentation/feed-change-webhook-relay.md) — Relay Cloud Run service architecture for Pub/Sub audit log forwarding.
* [Protobuf Schema Validation Guide](./documentation/PROTO_VALIDATION.md) — GCP Pub/Sub schema constraints and single root-level message validation.
* [Gemini Retry & Error Handling Guidelines](./documentation/gemini_retry_guidelines.md) — gRPC/HTTP status code mapping, transient vs permanent error classification, and FinishReason rules.

### ASR Model & Evaluation Infrastructure (`model/`)
* [ASR Evaluation & Notebook Guide](./ASR_CONTRIBUTING.md) — Setup for GPU/CPU evaluation Docker runtimes, GCE GPU VM provisioning, and notebook formatting.
* [Data Artifacts & Source Layout](./model/data/README.md) — Structure of dataset manifests, inference results, and data source connectors.
* [Canonical Data Manifest Contract](./model/data/manifests/README.md) — Unified JSONL specification for training and evaluation data.
* [Inference Manifest Specification](./model/data/inference_manifests/README.md) — Scorer-ready manifest contract combining reference text and model predictions.
* Data Source Connectors:
  * [Broadcastify Connector](./model/data_sources/broadcastify/README.md)
  * [Watch Duty Echo Connector](./model/data_sources/echo/README.md)
  * [Fire Notifications Connector](./model/data_sources/fire_notifications/README.md)
* Gemini Supervised Fine-Tuning (SFT) Framework:
  * [Gemini SFT Overview](./model/scripts/sft/README.md) — CLI overview for `gemini-sft` (`prepare`, `tune`, `eval`).
  * [SFT Documentation Index](./model/scripts/sft/docs/index.md) — Complete operator guide index.
  * [Operator Runbook](./model/scripts/sft/docs/runbook.md) — Step-by-step operational runbook for fine-tuning rounds.
  * [Run Configuration Guide](./model/scripts/sft/docs/configs.md) — TOML configuration reference.
  * [Metrics Glossary](./model/scripts/sft/docs/metrics.md) — WER, CER, token caps, and loss tracking.
  * [Artifact Reference](./model/scripts/sft/docs/artifacts.md) & [Artifact Hygiene](./model/scripts/sft/docs/hygiene.md) — GCS state layout and cleanup policies.

### Management & Proxy APIs (`frontend/api/` & `backend/services/`)
* [Frontend Proxy API Overview](./frontend/api/README.md) — Node/Express authentication and routing proxy service.
* [OpenAPI Spec Generation & Auth Injection](./frontend/api/README.md#openapi-specification-generation--google-auth-injection) — `tsoa` spec compilation and `x-google-auth` post-processing.
* [Google Cloud Identity RBAC Setup](./frontend/api/README.md#admin-group-membership-configuration-google-cloud-identity) — Workspace/Cloud Identity group authorization configuration.

### 4. Frontend UI Tool (`frontend/transcription-ui/`)
* [React + TypeScript + Vite UI](./frontend/transcription-ui/README.md) — Frontend application structure, routing, and developer setup.

### 5. Deployment Infrastructure (`radio-transcription-deployment/` & `terraform/`)
* [Deployment Infrastructure Repository](file:///usr/local/google/home/stephaniechew/watch_duty/radio-transcription-deployment/README.md) — Terraform modular architecture and GitHub Actions release workflows.
* [Dataflow IAM Runbook](file:///usr/local/google/home/stephaniechew/watch_duty/radio-transcription-deployment/docs/dataflow-iam-runbook.md) — GCP IAM roles, service accounts, and service agent configurations.
* [Dataflow Tuning Runbook](file:///usr/local/google/home/stephaniechew/watch_duty/radio-transcription-deployment/docs/dataflow-tuning.md) — Performance optimization, parallelism tuning, and key partitioning.

### 🧪 Testing & Quality Assurance
* [Component & API Integration Tests](./CONTRIBUTING.md#integration-and-e2e-tests) — Unit, component (testcontainers), API, and E2E test suites.
* [E2E Pipeline Regression Test Suite](./documentation/REGRESSION_TESTS.md) — Automated live environment regression suite and teardown utilities.

---
