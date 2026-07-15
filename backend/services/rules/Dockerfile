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

# 1. Install uv as the package manager
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/

# 2. Create virtual environment and copy project configuration files dynamically
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package rules-service

# 4. Copy necessary application source code
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/pipeline/storage/ ./backend/pipeline/storage/
COPY backend/pipeline/evaluation/ ./backend/pipeline/evaluation/
COPY backend/pipeline/schema_types/ ./backend/pipeline/schema_types/
COPY backend/services/audio_segments/ ./backend/services/audio_segments/
COPY backend/services/rules/ ./backend/services/rules/
# 5. Start the application
CMD exec uvicorn backend.services.rules.main:app --host 0.0.0.0 --port $PORT
