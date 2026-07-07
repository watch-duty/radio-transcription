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
    uv sync --frozen --no-dev --package echo-api

# 4. Copy necessary application source code
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/services/echo/ ./backend/services/echo/

# 5. Start the application
CMD exec functions-framework --source=backend/services/echo/main.py --target=verify_directory --signature-type=http --port=$PORT
