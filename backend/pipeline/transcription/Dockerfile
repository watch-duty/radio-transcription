FROM apache/beam_python312_sdk:2.56.0

# 1. Install ffmpeg and libomp5
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libomp5 \
    && rm -rf /var/lib/apt/lists/*

# 2. Install dependencies via uv
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/pyproject.toml

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

RUN uv sync --frozen --no-dev

# Copy the actual code to resolve local packages
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/transcription/ ./backend/pipeline/transcription/

# Set the entrypoint to the Beam worker harness
ENTRYPOINT ["/opt/apache/beam/boot"]
