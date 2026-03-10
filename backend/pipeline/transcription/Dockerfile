FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

COPY backend/pipeline/transcription/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/transcription/ ./backend/pipeline/transcription
COPY backend/pipeline/schema_types/ ./backend/pipeline/schema_types

CMD ["functions-framework", \
     "--source=backend/pipeline/transcription/main.py", \
     "--target=transcribe_audio", \
     "--signature-type=cloudevent", \
     "--port=8080"]
