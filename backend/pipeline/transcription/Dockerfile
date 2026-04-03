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

COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
WORKDIR /app

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/backend/pipeline/transcription/main.py"
# PYTHONPATH makes `backend.*` importable for both the launcher and Dataflow workers.
# Workers use the base image's system Python (/usr/local/bin/python3), so all deps
# must be installed there — not in a virtual env.
ENV PYTHONPATH="/app"

# Copy only dependency manifests first so this layer is cached independently of
# source changes. Only pyproject.toml/uv.lock changes bust the install step.
COPY pyproject.toml uv.lock ./
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/pyproject.toml

# Export the locked dependency graph to a flat requirements file and install
# into the system Python. --no-emit-project omits the local packages themselves
# (backend is handled via PYTHONPATH; we only need the third-party deps here).
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt && \
    pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip install --no-cache-dir grpcio-tools

# Copy full source after install — code changes don't bust the dep cache
COPY backend/ ./backend/
COPY protos/ ./protos

# Generate protobuf bindings
RUN mkdir -p ./backend/pipeline/schema_types && \
    touch ./backend/pipeline/schema_types/__init__.py && \
    python3 -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]

