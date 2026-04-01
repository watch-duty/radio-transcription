FROM python:3.13-slim

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
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/
RUN uv sync --frozen --no-dev

# 4. Copy necessary application source code
COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/pipeline/storage/ ./backend/pipeline/storage/
COPY backend/pipeline/schema_types/ ./backend/pipeline/schema_types/
COPY backend/services/transcripts/ ./backend/services/transcripts/

# 5. Start the application
CMD exec uvicorn backend.services.transcripts.main:app --host 0.0.0.0 --port $PORT
