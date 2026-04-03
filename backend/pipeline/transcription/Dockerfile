FROM gcr.io/dataflow-templates-base/python313-template-launcher-base:latest AS launcher

FROM apache/beam_python3.13_sdk:2.70.0

COPY --from=launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Install OS dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libomp5 \
        libc++1 \
        libc++abi1 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies via uv
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
WORKDIR /app

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app:/app/.venv/lib/python3.13/site-packages"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/backend/pipeline/transcription/main.py"

# Copy only the dependency manifests first so that uv sync is cached independently
# of source code changes. Only pyproject.toml/uv.lock changes bust this layer.
# The workspace member pyproject.toml must also be present for uv to resolve local deps.
COPY pyproject.toml uv.lock ./
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/pyproject.toml
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools

# Copy full source after install — code changes no longer bust the dep cache
COPY backend/ ./backend/
COPY protos/ ./protos

# Generate protobuf bindings
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

# The python_template_launcher reads FLEX_TEMPLATE_PYTHON_PY_FILE and configures
# Dataflow workers to use this same container image. No wrapper script needed.
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]

