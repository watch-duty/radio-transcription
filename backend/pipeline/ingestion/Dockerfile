FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg

COPY backend/pipeline/ingestion/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend/pipeline/ingestion/ ./ingestion

# Run Icecast Server
CMD ["python", "ingestion/icecast_collector.py"]
