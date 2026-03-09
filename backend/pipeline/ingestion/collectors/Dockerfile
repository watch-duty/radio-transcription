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

COPY backend/pipeline/ingestion/ ./ingestion
COPY backend/pipeline/storage/ ./storage

# Run Icecast Server
CMD ["python", "ingestion/collectors/icecast_collector.py"]
