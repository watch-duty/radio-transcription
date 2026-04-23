import torch
from nemo.collections.speechlm2.models import SALM

class CanaryQwenModel:
    def __init__(self):
        print("Loading NVIDIA Canary-Qwen-2.5B into GPU VRAM...")
        self.model = SALM.from_pretrained('nvidia/canary-qwen-2.5b').half().eval().to("cuda")
        
    def transcribe(self, audio_path: str, prompt: str) -> str:
        context_prompt = f"{prompt}\n\n{self.model.audio_locator_tag}"
        
        with torch.no_grad():
            answer_ids = self.model.generate(
                prompts=[
                    [
                        {
                            "role": "user",
                            "content": context_prompt,
                            "audio": [audio_path]
                        }
                    ]
                ],
                max_new_tokens=256,
            )
            
        # Decode the output safely
        if isinstance(answer_ids[0], str):
            transcript = answer_ids[0]
        else:
            transcript = self.model.tokenizer.ids_to_text(answer_ids[0].cpu())
            
        return transcript.strip()
