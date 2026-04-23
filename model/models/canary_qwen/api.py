import os
from fastapi import FastAPI, File, UploadFile, Form
from pydub import AudioSegment
import uvicorn
from model import CanaryQwenModel

app = FastAPI(title="Canary-Qwen 2.5B GPU Inference API")

# Load model on startup
model_wrapper = CanaryQwenModel()

@app.post("/transcribe")
async def transcribe(file: UploadFile = File(...), prompt: str = Form(None)):
    temp_path = f"temp_{file.filename}"
    clean_path = f"clean_{file.filename}.wav"
    
    # Save uploaded file
    with open(temp_path, "wb") as f:
        f.write(await file.read())
        
    try:
        print(f"Standardizing audio format for {file.filename} to 16kHz Mono...")
        # Force audio to 16,000 Hz and Mono using pydub
        audio = AudioSegment.from_file(temp_path)
        audio = audio.set_frame_rate(16000).set_channels(1)
        audio.export(clean_path, format="wav")
        
        custom_prompt = prompt if prompt else "You are an AI assistant helping to transcribe low quality radio audio from emergency services. Transcribe the audio accurately."
        
        print("Transcribing with Canary-Qwen...")
        transcript = model_wrapper.transcribe(clean_path, custom_prompt)
        
        return {"transcription": transcript}
        
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        if os.path.exists(clean_path):
            os.remove(clean_path)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
