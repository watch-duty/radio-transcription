import os
import torch
from fastapi import FastAPI, File, UploadFile, Form
from qwen_asr import Qwen3ASRModel
from transformers import logging
import uvicorn

app = FastAPI(title="Qwen3-ASR CPU Inference API")

logging.set_verbosity_error()

print("Loading Qwen3-ASR-0.6B into system RAM on CPU...")
MODEL_ID = os.getenv("MODEL_ID", "Qwen/Qwen3-ASR-0.6B")

model = Qwen3ASRModel.from_pretrained(
    MODEL_ID,
    dtype=torch.float32,
    device_map="cpu",
    max_new_tokens=256
)

@app.post("/transcribe")
async def transcribe(file: UploadFile = File(...), prompt: str = Form(None)):
    temp_path = f"temp_{file.filename}"
    with open(temp_path, "wb") as f:
        f.write(await file.read())

    try:
        print(f"Transcribing {file.filename}...")
        if prompt:
            results = model.transcribe(
                audio=temp_path,
                prompt=prompt
            )
        else:
            results = model.transcribe(
                audio=temp_path,
            )
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    return {"transcription": results}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
