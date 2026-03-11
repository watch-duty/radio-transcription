FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install ffmpeg for audio processing
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# 2. Install dependencies (generated from pyproject.toml via: uv export --no-hashes --no-dev --no-emit-project -o backend/pipeline/requirements.txt)
COPY backend/pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt grpcio-tools

# Copy the full package structure to preserve Python import paths
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/ingestion/ ./backend/pipeline/ingestion/
COPY backend/pipeline/storage/ ./backend/pipeline/storage/
COPY protos/ ./protos

# 3. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["python", "-m", "backend.pipeline.ingestion.collectors.icecast_collector"]
