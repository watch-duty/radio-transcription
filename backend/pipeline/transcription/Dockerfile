FROM apache/beam_python3.12_sdk:2.71.0

# Install OS dependencies: ffmpeg and libomp5
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libomp5 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies via uv
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
# We intentionally copy the entire backend/ directory before running uv sync.
# This ensures that ALL workspace sub-projects (e.g. transcription, ingestion) have
# their corresponding pyproject.toml files present. Without this, uv cannot resolve
# the local workspace dependencies defined in the root pyproject.toml.
COPY backend/ ./backend/

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

RUN uv sync --frozen --no-dev && uv pip install grpcio-tools

COPY protos/ ./protos

# Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

# Set the entrypoint to the Beam worker harness
ENTRYPOINT ["/opt/apache/beam/boot"]
