import os
import torch
from fastapi import FastAPI, File, UploadFile, Form
from pydub import AudioSegment
import uvicorn
from nemo.collections.speechlm2.models import SALM

app = FastAPI(title="Canary-Qwen 2.5B GPU Inference API")

print("Loading NVIDIA Canary-Qwen-2.5B into GPU VRAM...")
# Load the model directly to the GPU in standard float16 (FP16) precision
model = SALM.from_pretrained('nvidia/canary-qwen-2.5b').half().eval().to("cuda")

BASE_PHRASE_SET = [
    "copy", "copy that", "received", "affirmative", "affirm", "negative",
    "proceed", "responding", "responding to", "en route", "on scene",
    "on scene in the area", "available", "returning", "in service",
    "got a caller", "caller advising", "in quarters", "arrived", "arrived at",
    "go ahead", "back at", "stand by", "disregard", "clear", "emergency traffic",
    "engine", "tanker", "brush", "brush truck", "tender", "battalion",
    "squad", "ladder", "tower", "tower ladder", "medic", "ambulance",
    "rescue", "chief", "dispatch", "command", "pump", "pumper",
    "k", "okay", "branch", "chopper", "copter",
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
    "AIQ", "A I Q", "AOR", "A O R", "IC", "I C", "ICP", "I C P",
    "LAT", "RP", "R P", "SEAT", "TAC", "VFIRE", "VLAT", "LZ", "L Z", "medevac",
    "air attack", "air tactics", "helispot", "lead plane", "strike team",
    "control", "staging", "level one staging", "level two staging",
    "being toned", "box alarm", "cancel the balance", "chaparral",
    "exposure protection", "fire attack", "fire boss",
    "forward progress stopped", "forward rate of spread stopped",
    "heavy timber", "left flank", "light flashy fuels",
    "rate of spread", "right flank", "structure defense",
    "structure protection", "structures threatened",
    "terrain driven", "wind driven", "knockdown", "primary search",
    "secondary search", "all clear", "under control", "loss stopped"
]

TEN_CODES = ["10-4", "10-7", "10-8", "10-9", "10-20", "10-22", "10-23", "10-97"]

SPOKEN_TEN_CODES = [
    "ten four", "ten seven", "ten eight", "ten nine",
    "ten twenty", "ten twenty two", "ten twenty three", "ten ninety seven"
]

FINAL_PHRASE_LIST = BASE_PHRASE_SET + TEN_CODES + SPOKEN_TEN_CODES

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
        
        context_prompt = (
            f"{custom_prompt}\n\n"
            f"Pay close attention to these specific jargon words and codes: {', '.join(FINAL_PHRASE_LIST)}.\n\n"
            f"{model.audio_locator_tag}"
        )
        
        print("Transcribing with Canary-Qwen...")
        with torch.no_grad():
            answer_ids = model.generate(
                prompts=[
                    [
                        {
                            "role": "user",
                            "content": context_prompt,
                            "audio": [clean_path]
                        }
                    ]
                ],
                max_new_tokens=256,
            )
            
        # Decode the output safely
        if isinstance(answer_ids[0], str):
            transcript = answer_ids[0]
        else:
            transcript = model.tokenizer.ids_to_text(answer_ids[0].cpu())
            
        return {"transcription": transcript.strip()}
        
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        if os.path.exists(clean_path):
            os.remove(clean_path)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
