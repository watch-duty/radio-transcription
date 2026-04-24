import os
from contextlib import asynccontextmanager
import tempfile
from fastapi import FastAPI, File, UploadFile, Form, Request
from pydub import AudioSegment
import uvicorn
from model import CanaryQwenModel


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load model on startup
    print("Loading Canary-Qwen model via lifespan...")
    app.state.model = CanaryQwenModel()
    yield


app = FastAPI(title="Canary-Qwen 2.5B GPU Inference API", lifespan=lifespan)


@app.post("/transcribe")
async def transcribe(
    request: Request, file: UploadFile = File(...), prompt: str = Form(None)
):
    temp_path = None
    clean_path = None

    try:
        # Create temp file safely
        suffix = os.path.splitext(file.filename)[1] if file.filename else ".tmp"
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix
        ) as temp_file:
            temp_file.write(await file.read())
            temp_path = temp_file.name

        # Create a clean path for processed audio
        clean_path = temp_path + "_clean.wav"

        print(
            f"Standardizing audio format for {file.filename} to 16kHz Mono..."
        )
        audio = AudioSegment.from_file(temp_path)
        audio = audio.set_frame_rate(16000).set_channels(1)
        audio.export(clean_path, format="wav")

        custom_prompt = (
            prompt
            if prompt
            else "You are an AI assistant helping to transcribe low quality radio audio from emergency services. Transcribe the audio accurately."
        )

        print("Transcribing with Canary-Qwen...")
        model_wrapper = request.app.state.model
        transcript = model_wrapper.transcribe(clean_path, custom_prompt)

        return {"transcription": transcript}

    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        if clean_path and os.path.exists(clean_path):
            os.remove(clean_path)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
