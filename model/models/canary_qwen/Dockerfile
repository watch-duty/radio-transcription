FROM nvcr.io/nvidia/nemo:26.02.00

WORKDIR /app

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir fastapi uvicorn python-multipart pydub

COPY model/models/canary_qwen/api.py /app/api.py

EXPOSE 8000

CMD ["python", "api.py"]
