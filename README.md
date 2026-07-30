# Radio Transcription

A scalable, real-time open-source system designed for transcribing and managing emergency radio traffic.

---

## Repository Architecture

The `radio-transcription` repository and ecosystem contains 5 main offerings:

1. **[Radio Transcription Pipeline](#radio-transcription-pipeline)**: Real-time audio ingestion collectors, Apache Beam/Dataflow Voice Activity Detection (VAD) audio segmentation for continuous audio, transcription service, rules evaluation, and notification dispatch.
2. **[ASR Model & Evaluation Infrastructure](#asr-model--evaluation-infrastructure)**: Colabs and tooling supporting evaluation and fine tuning for open and hosted ASR models, like Whisper and Gemini.
3. **[Management & Proxy APIs](#management--proxy-apis)**: APIs for managing the resources driving the transcription pipeline (e.g. Feeds, Transcripts, Rules, and FE Proxy APIs).
4. **[Frontend UI Tool](#frontend-ui-tool)**: A React interface for users to monitor live audio streams, manage feed lifecycle states, configure alert rules, and view transcribed audio.
5. **[Deployment Infrastructure](#deployment-infrastructure)**: Terraform modules for provisioning a GCP environment with all of the tools listed above.

---

## Directory Structure

```
radio-transcription/
├── backend/
│   └── pipeline/             # Audio ingestion collectors, VAD segmentation, transcription, rules, and notification
├── frontend/
│   ├── api/                  # Express server hosting the FE Proxy API
│   ├── common/               # Shared TypeScript schemas and utilities
│   └── transcription-ui/     # React + TypeScript + Vite web UI
├── model/
│   ├── colabs/               # Model evaluation notebooks
│   ├── data/                 # Data artifacts for training
│   ├── data_sources/         # Scripts for fetching audio from supported audio sources (Broadcastify, Echo, Fire Notifications)
│   ├── scripts/sft/          # Gemini Supervised Fine-Tuning (SFT) process
│   └── src/                  # Shared functionality for inference across the different models
├── protos/                   # Shared Protocol Buffer schema definitions used throughout the pipeline
└── terraform/                # Infrastructure for pipeline, UI, and GPU provisioning
```

---

## Onboarding

* **[Getting Started Guide](./documentation/GETTING_STARTED.md)** — Setup and quick start instructions.
* **[Contributing Guide](./documentation/CONTRIBUTING.md)** — Pre-requisites, development workflows, pre-commit setup, and coding standards.
* **[AI Agent Instructions](./AGENTS.md)** — Guidelines for automated coding agents.

---

## Repository Documentation Index

### Radio Transcription Pipeline
* [Backend Pipeline Architecture](./backend/pipeline/README.md)
* [Collector Authoring & Failure Policy Guide](./backend/pipeline/ingestion/collectors/README.md)
* [Continuous Audio Segmentation Architecture](./backend/pipeline/segmentation/ARCHITECTURE.md)
* [Audio Segmentation Module Overview](./backend/pipeline/segmentation/README.md)
* [VAD Benchmarks](./backend/pipeline/segmentation/tests/VAD_BENCHMARKS.md)
* [Broadcastify Credential Rotation](./backend/pipeline/ingestion/broadcastify_credential_rotation/README.md)
* [Feed Change Webhook Relay](./documentation/feed-change-webhook-relay.md)
* [Protobuf Schema Validation Guide](./documentation/PROTO_VALIDATION.md)
* [Gemini Retry & Error Handling Guidelines](./documentation/gemini_retry_guidelines.md)
* [Feed Change Webhook Relay](./documentation/feed-change-webhook-relay.md)

### ASR Model & Evaluation Infrastructure
* [ASR Evaluation & Notebook Guide](./documentation/ASR_CONTRIBUTING.md)
* [Data Artifacts & Source Layout](./model/data/README.md)
* [Canonical Data Manifest Contract](./model/data/manifests/README.md)
* [Inference Manifest Specification](./model/data/inference_manifests/README.md)
* Data Source Connectors:
  * [Broadcastify Connector](./model/data_sources/broadcastify/README.md)
  * [Watch Duty Echo Connector](./model/data_sources/echo/README.md)
  * [Fire Notifications Connector](./model/data_sources/fire_notifications/README.md)
* Gemini Supervised Fine-Tuning (SFT) Framework:
  * [Gemini SFT Overview](./model/scripts/sft/README.md)
  * [SFT Documentation Index](./model/scripts/sft/docs/index.md)
  * [Operator Runbook](./model/scripts/sft/docs/runbook.md)
  * [Run Configuration Guide](./model/scripts/sft/docs/configs.md)
  * [Metrics Glossary](./model/scripts/sft/docs/metrics.md)
  * [Artifact Reference](./model/scripts/sft/docs/artifacts.md) & [Artifact Hygiene](./model/scripts/sft/docs/hygiene.md)

### Management & Proxy APIs
* [Frontend Proxy API Overview](./frontend/api/README.md)
* [OpenAPI Spec Generation & Auth Injection](./frontend/api/README.md#openapi-specification-generation--google-auth-injection)
* [Google Cloud Identity RBAC Setup](./frontend/api/README.md#admin-group-membership-configuration-google-cloud-identity)

### Frontend UI Tool
* [React UI](./frontend/transcription-ui/README.md)

<!-- TODO: Add in documentation for infrastructure when migration is complete. -->

### Local Development & Testing
* [Local Mock Audio Server](./documentation/local-dev-mock-audio.md) — Mocking incoming audio streams for local testing.
* [Component & API Integration Tests](./documentation/CONTRIBUTING.md#integration-and-e2e-tests)
* [E2E Pipeline Regression Test Suite](./documentation/REGRESSION_TESTS.md)
