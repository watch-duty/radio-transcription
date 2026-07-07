import logging
import tempfile
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlparse

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from faster_whisper import WhisperModel
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Load model on startup
logger.info("Loading Whisper model...")
model = WhisperModel("tiny", device="cpu", compute_type="int8")
logger.info("Model loaded successfully.")


class StorageClientHolder:
    client: storage.Client | None = None


# Initialize storage client
# This will automatically use the emulator if STORAGE_EMULATOR_HOST is set.
try:
    StorageClientHolder.client = storage.Client()
except Exception as e:
    logger.warning(
        "Could not initialize default storage client: "
        f"{e}. Will retry or fail on request."
    )


def download_blob(uri: str, local_path: str) -> None:
    if not StorageClientHolder.client:
        StorageClientHolder.client = storage.Client()

    parsed_uri = urlparse(uri)
    if parsed_uri.scheme != "gs":
        msg = (
            f"Unsupported URI scheme: {parsed_uri.scheme}. "
            "Only gs:// is supported."
        )
        raise ValueError(msg)

    bucket_name = parsed_uri.netloc
    blob_name = parsed_uri.path.lstrip("/")

    logger.info(f"Downloading {uri} to {local_path}")
    bucket = StorageClientHolder.client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)


@app.post("/transcribe")
async def transcribe(
    file: Annotated[UploadFile | None, File()] = None,
    uri: Annotated[
        str | None, Query(description="GCS URI (gs://bucket/object)")
    ] = None,
) -> dict[str, Any]:
    logger.info(
        "Received transcription request. "
        f"File: {file.filename if file else None}, URI: {uri}"
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
        segments, _ = model.transcribe(tmp_path, language="en")

        text = " ".join([segment.text for segment in segments])
        logger.info("Transcription completed.")

        return {"text": text.strip()}

    except Exception as e:
        logger.exception("Error during transcription")
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        if tmp_path:
            tmp_file = Path(tmp_path)
            if tmp_file.exists():  # noqa: ASYNC240
                tmp_file.unlink()  # noqa: ASYNC240


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy"}
