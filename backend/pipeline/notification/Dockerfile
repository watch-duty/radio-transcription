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
COPY backend/pipeline/notification/ ./backend/pipeline/notification
COPY backend/pipeline/common/__init__.py ./backend/pipeline/common/__init__.py
COPY backend/pipeline/common/storage/ ./backend/pipeline/common/storage
COPY protos/ ./protos

# 2. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/notification/send_notification.py", \
     "--target=send_notification", \
     "--signature-type=cloudevent"]
