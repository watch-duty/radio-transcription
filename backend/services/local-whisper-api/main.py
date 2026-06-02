from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from faster_whisper import WhisperModel
import tempfile
import os
import logging
from google.cloud import storage
from urllib.parse import urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Load model on startup
logger.info("Loading Whisper model...")
model = WhisperModel("tiny", device="cpu", compute_type="int8")
logger.info("Model loaded successfully.")

# Initialize storage client
# This will automatically use the emulator if STORAGE_EMULATOR_HOST is set.
try:
    storage_client = storage.Client()
except Exception as e:
    logger.warning(
        f"Could not initialize default storage client: {e}. Will retry or fail on request."
    )
    storage_client = None


def download_blob(uri: str, local_path: str):
    global storage_client
    if not storage_client:
        storage_client = storage.Client()

    parsed_uri = urlparse(uri)
    if parsed_uri.scheme != "gs":
        raise ValueError(
            f"Unsupported URI scheme: {parsed_uri.scheme}. Only gs:// is supported."
        )

    bucket_name = parsed_uri.netloc
    blob_name = parsed_uri.path.lstrip("/")

    logger.info(f"Downloading {uri} to {local_path}")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)


@app.post("/transcribe")
async def transcribe(
    file: UploadFile = File(None),
    uri: str = Query(None, description="GCS URI (gs://bucket/object)"),
):
    logger.info(
        f"Received transcription request. File: {file.filename if file else None}, URI: {uri}"
    )

    if not file and not uri:
        raise HTTPException(
            status_code=400, detail="Either file or uri must be provided."
        )

    tmp_path = None
    try:
        if file:
            file_bytes = await file.read()
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".tmp"
            ) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name
        elif uri:
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".tmp"
            ) as tmp:
                tmp_path = tmp.name
            download_blob(uri, tmp_path)

        logger.info(f"Transcribing file {tmp_path}")
        segments, info = model.transcribe(tmp_path)

        text = " ".join([segment.text for segment in segments])
        logger.info("Transcription completed.")

        return {
            "text": text.strip(),
            "language": info.language,
            "language_probability": info.language_probability,
        }

    except Exception as e:
        logger.error(f"Error during transcription: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


@app.get("/health")
async def health():
    return {"status": "healthy"}
