FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
RUN pip install --no-cache-dir qwen-asr accelerate fastapi uvicorn python-multipart sympy

COPY model/models/qwen/api.py /app/api.py

EXPOSE 8000

CMD ["python", "api.py"]
