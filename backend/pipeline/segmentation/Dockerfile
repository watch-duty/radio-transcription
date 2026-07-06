# syntax=docker/dockerfile:1.9-labs
# Use Apache Beam Python SDK as the base image
# IMPORTANT: When updating the version tag here, you must also update the
# apache-beam dependency version in backend/pipeline/segmentation/pyproject.toml.
FROM apache/beam_python3.13_sdk:2.74.0


# Install OS dependencies needed at runtime
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libatomic1 \
    && rm -rf /var/lib/apt/lists/*

# Copy uv from the official image
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/

# Copy the Flex Template Launcher from Google's base image
COPY --from=gcr.io/dataflow-templates-base/python313-template-launcher-base:latest /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

WORKDIR /app

# Copy lockfiles and manifests
COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./

# Set environment variables for uv to use system python
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
ENV UV_SYSTEM_PYTHON=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1

# Install dependencies using uv sync directly into system python
RUN uv sync --frozen --no-dev --no-install-workspace --package segmentation-pipeline && \
    uv export --group build --locked -o build-requirements.txt && \
    uv pip install -r build-requirements.txt

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/backend/pipeline/segmentation/main.py"
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Copy full source
COPY backend/ ./backend/
COPY protos/ ./protos

# Generate protobuf bindings
RUN mkdir -p ./backend/pipeline/schema_types && \
    touch ./backend/pipeline/schema_types/__init__.py && \
    python3 -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto && \
    mkdir -p .betterproto_out && \
    uv run --with "betterproto[compiler]>=2.0.0b6" python3 -m grpc_tools.protoc -I=protos --python_betterproto_out=.betterproto_out protos/streaming_state.proto && \
    cp .betterproto_out/streaming/pipeline/state/v1/__init__.py backend/pipeline/schema_types/streaming_state.py && \
    rm -rf .betterproto_out

# Copy and set the entrypoint script
COPY backend/pipeline/segmentation/entrypoint.sh /opt/google/dataflow/entrypoint.sh
RUN chmod +x /opt/google/dataflow/entrypoint.sh

ENTRYPOINT ["/opt/google/dataflow/entrypoint.sh"]
