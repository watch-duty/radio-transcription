FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# WIP check that we need this
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libsndfile1 \
    && rm -rf /var/lib/apt/lists/*

COPY backend/pipeline/ingestion/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend/pipeline/ingestion/ ./ingestion

# Run the collector
CMD ["python", "ingestion/icecast_collector.py"]
