FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg

COPY backend/pipeline/ingestion/collectors/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the full package structure to preserve Python import paths
COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/ingestion/ ./backend/pipeline/ingestion/
COPY backend/pipeline/storage/ ./backend/pipeline/storage/
COPY backend/pipeline/schema_types/ ./backend/pipeline/schema_types
RUN find backend/pipeline/schema_types/ -name "*.py"

CMD ["python", "-m", "backend.pipeline.ingestion.collectors.icecast_collector"]
