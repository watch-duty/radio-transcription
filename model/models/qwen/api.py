import os
from contextlib import asynccontextmanager
import tempfile
import torch
from fastapi import FastAPI, File, UploadFile, Form, Request
from qwen_asr import Qwen3ASRModel
from transformers import logging
import uvicorn

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load model on startup
    logging.set_verbosity_error()
    print("Loading Qwen3-ASR-0.6B into system RAM on CPU via lifespan...")
    MODEL_ID = os.getenv("MODEL_ID", "Qwen/Qwen3-ASR-0.6B")
    app.state.model = Qwen3ASRModel.from_pretrained(
        MODEL_ID,
        dtype=torch.float32,
        device_map="cpu",
        max_new_tokens=256
    )
    yield

app = FastAPI(title="Qwen3-ASR CPU Inference API", lifespan=lifespan)

@app.post("/transcribe")
async def transcribe(request: Request, file: UploadFile = File(...), prompt: str = Form(None)):
    temp_path = None
    
    try:
        # Create temp file safely
        suffix = os.path.splitext(file.filename)[1] if file.filename else ".tmp"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            temp_file.write(await file.read())
            temp_path = temp_file.name
            
        print(f"Transcribing {file.filename}...")
        model = request.app.state.model
        
        if prompt:
            results = model.transcribe(
                audio=temp_path,
                prompt=prompt
            )
        else:
            results = model.transcribe(
                audio=temp_path,
            )
            
        return {"transcription": results}
        
    finally:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
