FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install ffmpeg for audio processing
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# 2. Install dependencies
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools

# Copy the full package structure to preserve Python import paths
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/pipeline/ingestion/ ./backend/pipeline/ingestion/
COPY backend/pipeline/storage/ ./backend/pipeline/storage/
COPY protos/ ./protos

# 3. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["python", "-m", "backend.pipeline.ingestion.collectors.icecast_collector"]
