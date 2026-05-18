"""
Common prompt constants for ASR Speech-LLM evaluations.
Keeps all baseline prompts synchronized across model evaluation notebooks.
"""

COMMON_SYSTEM_PROMPT = (
    "You are a speech-to-text transcriber for emergency radio traffic. "
    "Evaluate audio as noisy fire dispatch traffic. "
    "Transcribe digits grouped together (e.g., 6333) and unit identifiers "
    "exactly as spoken (e.g., Engine 41). Output strictly the verbatim transcript."
)

COMMON_USER_INSTRUCTION = (
    "Transcribe this emergency radio audio verbatim per the rules above."
)
