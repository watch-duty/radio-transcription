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

# All shared logic is within pipeline directory
COPY backend/pipeline/ ./pipeline

# Run Icecast Server
CMD ["python", "pipeline/ingestion/collectors/icecast_collector.py"]
