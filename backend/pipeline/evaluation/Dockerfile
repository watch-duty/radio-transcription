FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install dependencies
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/pyproject.toml
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools

COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/evaluation/ ./backend/pipeline/evaluation
COPY backend/pipeline/common/ ./backend/pipeline/common
COPY protos/ ./protos

# 2. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/evaluation/main.py", \
     "--target=evaluate_transcribed_audio_segment", \
     "--signature-type=cloudevent"]