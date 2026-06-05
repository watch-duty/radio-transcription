# syntax=docker/dockerfile:1.9-labs
FROM python:3.13-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV WORKDIR=/app
ENV PYTHONPATH=/app
ENV PORT=8080

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package transcripts-service && uv pip install grpcio-tools

COPY protos/ ./protos

# Create directory for generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/pipeline/storage/ ./backend/pipeline/storage/
COPY backend/services/transcripts/ ./backend/services/transcripts/

CMD exec uvicorn backend.services.transcripts.main:app --host 0.0.0.0 --port $PORT
