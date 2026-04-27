FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# ---------------------------------------------------------------------------
# HANDOFF: cloud-init prerequisites (must land in the infrastructure PR)
# ---------------------------------------------------------------------------
# The container cannot set these from inside; they must be applied at VM-level
# via cloud-init in the follow-up Terraform PR against radio-transcription-deployment.
# HANDOFF: net.ipv4.ip_local_port_range = "10000 65535"
# HANDOFF: net.ipv4.tcp_keepalive_time = 60
# HANDOFF: net.ipv4.tcp_keepalive_intvl = 10
# HANDOFF: net.ipv4.tcp_keepalive_probes = 3
# HANDOFF: container --stop-timeout >= 100 s (90 s graceful budget + ~10 s teardown
#          vs the GCE 120 s ACPI cap)
# ---------------------------------------------------------------------------

# 1. Install ffmpeg for audio processing
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# 2. Install dependencies
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
# We intentionally copy the entire backend/ directory before running uv sync.
# This ensures that ALL workspace sub-projects (e.g. transcription, ingestion) have
# their corresponding pyproject.toml files present. Without this, uv cannot resolve
# the local workspace dependencies defined in the root pyproject.toml.
COPY backend/ ./backend/
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools
COPY protos/ ./protos

# 3. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["python", "-m", "backend.pipeline.ingestion.main"]
