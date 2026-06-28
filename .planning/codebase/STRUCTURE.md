# Structure

## Top Level

```text
.
├── backend/
├── frontend/
├── model/
├── integration_tests/
├── local_dev/
├── protos/
├── scripts/
├── terraform/
├── documentation/
├── .github/
├── docker-compose.yml
├── asr-eval-docker-compose.yml
├── pyproject.toml
├── uv.lock
├── .mise.toml
├── .tool-versions
├── README.md
├── CONTRIBUTING.md
├── ASR_CONTRIBUTING.md
└── CONTEXT.md
```

## Backend

```text
backend/
├── pipeline/
│   ├── common/
│   ├── evaluation/
│   ├── ingestion/
│   ├── normalization/
│   ├── notification/
│   ├── schema_types/
│   ├── segmentation/
│   ├── storage/
│   └── transcription/
├── services/
│   ├── audio_segments/
│   ├── feeds/
│   ├── local-whisper-api/
│   └── rules/
└── scripts/
```

Backend conventions:

- Pipeline stages live under `backend/pipeline/<stage>`.
- Reusable backend/cloud/auth/logging/tracing helpers live in
  `backend/pipeline/common`.
- Database stores and SQL-facing logic live in `backend/pipeline/storage`.
- Generated protobuf code lives in `backend/pipeline/schema_types`.
- FastAPI domain services live in `backend/services`.
- Each packaged workspace member has its own `pyproject.toml`.
- Dockerfiles are colocated with service directories.

## Frontend

```text
frontend/
├── common/
├── api/
└── transcription-ui/
```

Frontend conventions:

- `frontend/common` builds shared TypeScript used by API and UI.
- `frontend/api` is the proxy API/BFF and owns generated TSOA route/spec output.
- `frontend/transcription-ui` is the React application.
- Each frontend package has its own `package.json` and `yarn.lock`.

## Model

```text
model/
├── src/
│   ├── common/
│   │   └── gemini/
│   └── gemini_sft/
├── tests/
│   ├── common/
│   └── gemini_sft/
├── scripts/
│   └── sft/
├── colabs/
├── data_sources/
├── research/
├── trained_checkpoints/
├── notebook_docker/
├── nemo_docker/
├── pyproject.toml
└── uv.lock
```

Model conventions:

- Shared importable code belongs in `model/src`, not only notebooks.
- Maintained notebooks should import from `common` or `common.gemini`.
- Gemini SFT operator code belongs in `model/src/gemini_sft`.
- One-off or operator scripts live under `model/scripts`.
- Research notes and local run configs under `model/research` are not the
  durable state for SFT runs; GCS run prefixes are authoritative.

## Integration Tests

```text
integration_tests/
├── api/
├── e2e/
├── storage/
├── conftest.py
├── feed_utils.py
├── test_utils.py
└── utils.py
```

Integration test levels:

- `storage`: testcontainers-backed component tests for stores.
- `api`: HTTP tests against running API services.
- `e2e`: full system flow tests against Docker Compose services.

## Infrastructure

```text
terraform/modules/
├── alloydb/
├── asr_evaluation/
├── cloud_function/
├── container_mig/
├── gcs_bucket/
└── memorystore_for_redis/
```

The AlloyDB module includes SQL migrations and CI guard scripts. The
`asr_evaluation` module provisions operator GPU/CPU VM infrastructure for ASR
experiments.

## Generated And Local Artifacts

Treat these as generated/local unless a task explicitly says otherwise:

- `.venv/`, `model/.venv/`
- `.ruff_cache/`, `.pytest_cache/`
- `results/`
- generated protobuf outputs in `backend/pipeline/schema_types`
- local SFT TOML files ending in `.local.toml`
- local inference manifests under `model/data/inference_manifests`

The current worktree had unrelated dirty/untracked experiment artifacts at map
time. They were intentionally excluded from this codebase map.
