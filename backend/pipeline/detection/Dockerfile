# ---------- builder: compile protobuf files ----------
FROM python:3.13-slim AS builder

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy transcription pyproject.toml to satisfy uv workspace requirement
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/
RUN uv sync --frozen --no-dev --group build

COPY protos/ ./protos
RUN mkdir -p ./backend/pipeline/schema_types \
    && touch ./backend/pipeline/schema_types/__init__.py \
    && python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

# ---------- runtime: production image ----------
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install system deps (ffmpeg for FLAC decoding at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

# 2. Install production-only Python dependencies (no grpcio-tools)
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy transcription pyproject.toml to satisfy uv workspace requirement
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/
RUN uv sync --frozen --no-dev

# 3. Copy source files
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/detection/ ./backend/pipeline/detection
COPY backend/pipeline/common/ ./backend/pipeline/common

# 4. Copy pre-compiled protobuf files from builder
COPY --from=builder /app/backend/pipeline/schema_types/ ./backend/pipeline/schema_types

CMD ["functions-framework", \
     "--source=backend/pipeline/detection/normalization_handler.py", \
     "--target=normalize", \
     "--signature-type=cloudevent"]
