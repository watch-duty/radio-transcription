# --- Stage 1: The Builder ---
FROM python:3.13-slim AS builder

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/

WORKDIR /app

# Copy lockfiles and manifests
COPY pyproject.toml uv.lock ./
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/pyproject.toml

# Use uv to export and then install into the system site-packages of the builder.
# We keep your logic to strip editable installs (-e) for workspace members.
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt && \
    grep -v '^-e ' /tmp/requirements.txt > /tmp/requirements_pinned.txt && \
    uv pip install --system --no-cache -r /tmp/requirements_pinned.txt && \
    uv pip install --system --no-cache grpcio-tools

# --- Stage 2: The Dataflow Worker ---
FROM apache/beam_python3.13_sdk:2.70.0

# Copy the launcher from the google-provided base
COPY --from=gcr.io/dataflow-templates-base/python313-template-launcher-base:latest \
     /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Copy the installed packages from the builder stage
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Install OS dependencies needed at runtime
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libomp5 \
        libc++1 \
        libc++abi1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/backend/pipeline/transcription/main.py"
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Copy full source
COPY backend/ ./backend/
COPY protos/ ./protos

# Generate protobuf bindings
RUN mkdir -p ./backend/pipeline/schema_types && \
    touch ./backend/pipeline/schema_types/__init__.py && \
    python3 -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
