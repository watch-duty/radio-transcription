# Use Apache Beam Python SDK as the base image
# IMPORTANT: When updating the version tag here, you must also update the
# apache-beam dependency version in backend/pipeline/transcription/pyproject.toml.
FROM apache/beam_python3.13_sdk:2.72.0


# Install OS dependencies needed at runtime
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libomp5 \
    && rm -rf /var/lib/apt/lists/*

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/

# Copy the Flex Template Launcher from Google's base image
COPY --from=gcr.io/dataflow-templates-base/python313-template-launcher-base:latest /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

WORKDIR /app

# Copy lockfiles and manifests
COPY pyproject.toml uv.lock ./
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/pyproject.toml

# Set environment variables for uv to use system python
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
ENV UV_SYSTEM_PYTHON=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1

# Install dependencies using uv sync directly into system python
RUN uv sync --frozen --no-dev --no-install-workspace --package transcription-pipeline && \
    uv pip install grpcio-tools

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

# Copy and set the entrypoint script
COPY backend/pipeline/transcription/entrypoint.sh /opt/google/dataflow/entrypoint.sh
RUN chmod +x /opt/google/dataflow/entrypoint.sh

ENTRYPOINT ["/opt/google/dataflow/entrypoint.sh"]
