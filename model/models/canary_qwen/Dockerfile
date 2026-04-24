FROM nvcr.io/nvidia/nemo:24.12

WORKDIR /app

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastapi uvicorn python-multipart pydub

# Copy all files from the directory
COPY model/models/canary_qwen/ /app/

EXPOSE 8000

CMD ["python", "api.py"]
