FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV WORKDIR=/app
ENV PYTHONPATH=/app
ENV PORT=8080

WORKDIR /app

# 1. Install uv as the package manager
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/

# 2. Copy project configuration files
COPY pyproject.toml uv.lock ./

# 3. Create virtual environment and install dependencies
# We no longer need grpcio-tools
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

# Copy transcription pyproject.toml to satisfy uv workspace requirement
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/
RUN uv sync --frozen --no-dev

# 4. Copy necessary application source code
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/pipeline/rules/ ./backend/pipeline/rules/

# 5. Start the application
CMD exec uvicorn backend.pipeline.rules.main:app --host 0.0.0.0 --port $PORT
